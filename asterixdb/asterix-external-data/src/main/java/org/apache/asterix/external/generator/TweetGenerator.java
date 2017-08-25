/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.generator;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.external.generator.DataGenerator.TweetMessage;
import org.apache.asterix.external.generator.DataGenerator.TwitterUser;
import org.apache.asterix.external.util.IDGenerator;
import org.apache.asterix.external.util.RandomSet;

public class TweetGenerator {
    private static final Logger LOGGER = Logger.getLogger(TweetGenerator.class.getName());

    public static final String KEY_GUID_SEED = "guid-seed";
    public static final String KEY_GUID_INCREMENT = "guid-increment";
    public static final String KEY_DURATION = "duration";
    public static final String KEY_TPS = "tps";
    public static final String KEY_VERBOSE = "verbose";
    public static final String KEY_PERCENTAGE_UPDATES = "updates-percent";
    public static final String KEY_PERCENTAGE_DELETES = "deletes-percent";
    public static final String KEY_CHANGE_FEED = "feed-type";
    public static final int INFINITY = 0;

    public static final String OUTPUT_FORMAT = "output-format";
    public static final String OUTPUT_FORMAT_ARECORD = "arecord";
    public static final String OUTPUT_FORMAT_ADM_STRING = "adm-string";

    private static final int DEFAULT_DURATION = INFINITY;

    protected final int duration;
    protected DataGenerator<? extends TweetMessage, ? extends TwitterUser>.TweetMessageIterator tweetIterator =
            null;
    protected final int partition;
    protected long tweetDeleteCount = 0;
    protected long tweetUpdateCount = 0;
    protected long tweetInsertCount = 0;
    protected int frameTweetCount = 0;
    protected int numFlushedTweets = 0;
    private final ByteBuffer outputBuffer = ByteBuffer.allocate(32 * 1024);
    private final List<OutputStream> subscribers;
    private final Object lock = new Object();
    private final List<OutputStream> subscribersForRemoval = new ArrayList<OutputStream>();
    private double updatesPercentage;
    private double deletesPercentage;
    private final boolean useChangeFeed;
    private Random r;
    private RandomSet<Long> currentIds;
    private RandomSet<Long> batchIds;

    public TweetGenerator(Map<String, String> configuration, int partition, DataGenerator dataGenerator,
            IDGenerator idGenerator) {
        this.partition = partition;
        String value = configuration.get(KEY_DURATION);
        this.duration = value != null ? Integer.parseInt(value) : DEFAULT_DURATION;
        value = configuration.get(KEY_PERCENTAGE_DELETES);
        deletesPercentage = value != null ? Double.parseDouble(value) : 0.0;
        value = configuration.get(KEY_PERCENTAGE_UPDATES);
        updatesPercentage = value != null ? Double.parseDouble(value) : 0.0;
        value = configuration.get(KEY_GUID_SEED);
        Long seed = value != null ? Long.parseLong(value) : -1;
        value = configuration.get(KEY_CHANGE_FEED);
        useChangeFeed = value != null ? Boolean.parseBoolean(value) : false;
        this.tweetIterator = dataGenerator.getTweetIterator(duration, idGenerator);
        this.subscribers = new ArrayList<OutputStream>();
        currentIds = new RandomSet<>();
        batchIds = new RandomSet<>();
        r = new Random(seed + partition);
    }

    private void writeTweetString(TweetMessage tweetMessage) throws IOException {
        StringBuilder recordBuilder = new StringBuilder();
        if (useChangeFeed) {
            Long id = tweetMessage.getTweetid();
            recordBuilder.append("\"").append(tweetMessage.toString().replace("\"", "\"\"")).append("\"");
            double draw = r.nextDouble();
            if (draw < deletesPercentage && currentIds.size() > 0) {
                id = currentIds.pollRandom(r);
                //reset & overwrite recordBuilder
                recordBuilder.setLength(0);
                recordBuilder.append(id).append(",");
                tweetDeleteCount++;
            } else {
                if (draw >= deletesPercentage && draw < (deletesPercentage + updatesPercentage)
                        && currentIds.size() > 0) {
                    id = currentIds.pollRandom(r);
                    tweetUpdateCount++;
                    batchIds.add(id);
                } else {
                    batchIds.add(id);
                    tweetInsertCount++;
                }
                recordBuilder.insert(0, Long.toString(id) + ",");
            }
        } else {
            recordBuilder.append(tweetMessage);
        }
        recordBuilder.append("\r\n");
        byte[] b = recordBuilder.toString().getBytes();
        if ((outputBuffer.position() + b.length) > outputBuffer.limit()) {
            flush();
            outputBuffer.put(b);
        } else {
            outputBuffer.put(b);
        }
        frameTweetCount++;
    }

    public void forceFlush() throws IOException {
        if (outputBuffer.position() > 0) {
            flush();
        }
    }

    private void flush() throws IOException {
        outputBuffer.flip();
        synchronized (lock) {
            for (OutputStream os : subscribers) {
                try {
                    os.write(outputBuffer.array(), 0, outputBuffer.limit());
                } catch (Exception e) {
                    LOGGER.info("OutputStream failed. Add it into the removal list.");
                    subscribersForRemoval.add(os);
                }
            }
            if (!subscribersForRemoval.isEmpty()) {
                subscribers.removeAll(subscribersForRemoval);
                subscribersForRemoval.clear();
            }
        }
        numFlushedTweets += frameTweetCount;
        frameTweetCount = 0;
        outputBuffer.position(0);
        outputBuffer.limit(32 * 1024);
    }

    public boolean generateNextBatch(int numTweets) throws IOException{
        boolean moreData = tweetIterator.hasNext();
        if (!moreData || numTweets <= frameTweetCount) {
            if (outputBuffer.position() > 0) {
                flush();
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Reached end of batch. Tweet Count [" + partition + "]" + getTweetCount() + ": "
                        + tweetInsertCount + " inserted, " + tweetUpdateCount + " updated, " + tweetDeleteCount
                        + " deleted");
            }
            return false;
        } else {
            int count = 0;
            while (count < numTweets) {
                writeTweetString(tweetIterator.next());
                count++;
            }
            //move generated in a batch IDs to current IDs
            currentIds.addAll(batchIds);
            batchIds.clear();
            return true;
        }
    }

    public int getNumFlushedTweets() {
        return numFlushedTweets;
    }

    public int getFrameTweetCount() {
        return frameTweetCount;
    }

    public void registerSubscriber(OutputStream os) {
        synchronized (lock) {
            subscribers.add(os);
        }
    }

    public void deregisterSubscribers(OutputStream os) {
        synchronized (lock) {
            subscribers.remove(os);
        }
    }

    public void close() throws IOException {
        synchronized (lock) {
            for (OutputStream os : subscribers) {
                os.close();
            }
        }
    }

    public boolean isSubscribed() {
        return !subscribers.isEmpty();
    }

    public long getTweetCount() {
        return tweetInsertCount - tweetDeleteCount;
    }

    public void resetDurationAndFlushedTweetCount(int duration) {
        tweetIterator.resetDuration(duration);
        numFlushedTweets = 0;
        tweetDeleteCount = 0;
        tweetUpdateCount = 0;
        tweetInsertCount = 0;

    }
}
