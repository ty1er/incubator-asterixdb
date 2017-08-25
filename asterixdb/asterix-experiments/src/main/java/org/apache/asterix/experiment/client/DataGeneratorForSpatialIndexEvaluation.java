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

package org.apache.asterix.experiment.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.asterix.external.generator.DataGenerator;
import org.apache.asterix.external.util.IDGenerator;

class TweetMessageSpatialIndex extends DataGenerator.TweetMessage {

    private static int NUM_BTREE_EXTRA_FIELDS = 8;

    private long tweetid;
    private DataGenerator.TwitterUser user;
    private DataGenerator.Point senderLocation;
    private DataGenerator.DateTime sendTime;
    private List<String> referredTopics;
    private DataGenerator.Message messageText;
    private int[] btreeExtraFields;
    private String dummySizeAdjuster;

    public TweetMessageSpatialIndex(int extraFieldsNum) {
        this.btreeExtraFields = new int[extraFieldsNum];
    }

    public TweetMessageSpatialIndex(long tweetid, DataGenerator.TwitterUser user, DataGenerator.Point senderLocation,
            DataGenerator.DateTime sendTime, List<String> referredTopics, DataGenerator.Message messageText,
            int[] btreeExtraFields, int extraFieldsNum, String dummySizeAdjuster) {
        this.btreeExtraFields = new int[extraFieldsNum];
        reset(tweetid, user, senderLocation, sendTime, referredTopics, messageText, btreeExtraFields,
                dummySizeAdjuster);
    }

    private void setBtreeExtraFields(int[] fVal) {
        if (btreeExtraFields.length != fVal.length) {
            throw new IllegalArgumentException(
                    "Number of extra field initializers does not match the nuber of extra fields");
        }
        for (int i = 0; i < btreeExtraFields.length; ++i) {
            btreeExtraFields[i] = fVal[i];
        }
    }

    public void reset(long tweetid, DataGenerator.TwitterUser user, DataGenerator.Point senderLocation,
            DataGenerator.DateTime sendTime, List<String> referredTopics, DataGenerator.Message messageText,
            int[] btreeExtraFields, String dummySizeAdjuster) {
        this.tweetid = tweetid;
        this.user = user;
        this.senderLocation = senderLocation;
        this.sendTime = sendTime;
        this.referredTopics = referredTopics;
        this.messageText = messageText;
        setBtreeExtraFields(btreeExtraFields);
        this.dummySizeAdjuster = dummySizeAdjuster;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("\"tweetid\":");
        builder.append("int64(\"" + tweetid + "\")");
        builder.append(",");
        builder.append("\"user\":");
        builder.append(user);
        builder.append(",");
        builder.append("\"sender-location\":");
        builder.append(senderLocation);
        builder.append(",");
        builder.append("\"send-time\":");
        builder.append(sendTime);
        builder.append(",");
        builder.append("\"referred-topics\":");
        builder.append("{{");
        for (String topic : referredTopics) {
            builder.append("\"" + topic + "\"");
            builder.append(",");
        }
        if (referredTopics.size() > 0) {
            builder.deleteCharAt(builder.lastIndexOf(","));
        }
        builder.append("}}");
        builder.append(",");
        builder.append("\"message-text\":");
        builder.append("\"");
        for (int i = 0; i < messageText.getLength(); i++) {
            builder.append(messageText.charAt(i));
        }
        builder.append("\"");
        builder.append(",");
        for (int i = 0; i < btreeExtraFields.length; ++i) {
            builder.append("\"btree-extra-field" + (i + 1) + "\":");
            builder.append(btreeExtraFields[i]);
            if (i != btreeExtraFields.length - 1) {
                builder.append(",");
            }
        }
        builder.append(",");
        builder.append("\"dummy-size-adjuster\":");
        builder.append("\"");
        builder.append(dummySizeAdjuster);
        builder.append("\"");
        builder.append("}");
        return new String(builder);
    }

    public long getTweetid() {
        return tweetid;
    }

    public void setTweetid(long tweetid) {
        this.tweetid = tweetid;
    }

    public DataGenerator.TwitterUser getUser() {
        return user;
    }

    public void setUser(DataGenerator.TwitterUser user) {
        this.user = user;
    }

    public DataGenerator.Point getSenderLocation() {
        return senderLocation;
    }

    public void setSenderLocation(DataGenerator.Point senderLocation) {
        this.senderLocation = senderLocation;
    }

    public DataGenerator.DateTime getSendDateTime() {
        return sendTime;
    }

    public void setSendTime(DataGenerator.DateTime sendTime) {
        this.sendTime = sendTime;
    }

    public List<String> getReferredTopics() {
        return referredTopics;
    }

    public void setReferredTopics(List<String> referredTopics) {
        this.referredTopics = referredTopics;
    }

    public DataGenerator.Message getMessageText() {
        return messageText;
    }

    public void setMessageText(DataGenerator.Message messageText) {
        this.messageText = messageText;
    }

}

public class DataGeneratorForSpatialIndexEvaluation
        extends DataGenerator<TweetMessageSpatialIndex, DataGenerator.TwitterUser> {

    public static final String DUMMY_SIZE_ADJUSTER =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    public static int NUM_BTREE_EXTRA_FIELDS = 18;

    public static class SpatialIndexInitializationInfo extends DataGenerator.InitializationInfo {
        public SpatialIndexInitializationInfo() {
            super(new TweetMessageSpatialIndex(NUM_BTREE_EXTRA_FIELDS), new TwitterUser(), new Random());
        }
    }

    private LocationGeneratorFromOpenStreetMapData locationGenFromOpenStreetMapData;

    public DataGeneratorForSpatialIndexEvaluation(SpatialIndexInitializationInfo info) {
        super(info);
    }

    public DataGeneratorForSpatialIndexEvaluation(SpatialIndexInitializationInfo info, String openStreetMapFilePath,
            int locationSampleInterval) {
        this(info);
        if (openStreetMapFilePath != null) {
            locationGenFromOpenStreetMapData = new LocationGeneratorFromOpenStreetMapData();
            locationGenFromOpenStreetMapData.intialize(openStreetMapFilePath, locationSampleInterval);
            randLocationGen = null;
        }
    }

    @Override
    public TweetMessageIterator getTweetIterator(int duration, IDGenerator idGenerator) {
        return new TweetMessageSpatialIndexIterator(duration, idGenerator);
    }

    class TweetMessageSpatialIndexIterator extends TweetMessageIterator {

        public TweetMessageSpatialIndexIterator(int duration, IDGenerator idGen) {
            super(duration, idGen);
        }

        @Override
        public TweetMessageSpatialIndex next() {
            getTwitterUser(null);
            Message message = randMessageGen.getNextRandomMessage();
            Point location = randLocationGen != null ? randLocationGen.getRandomPoint()
                    : locationGenFromOpenStreetMapData.getNextPoint();
            DataGenerator.DateTime sendTime = randDateGen.getNextRandomDatetime();
            int[] btreeExtraFieldArray = new int[NUM_BTREE_EXTRA_FIELDS];
            Arrays.fill(btreeExtraFieldArray, random.nextInt());
            twMessage.reset(idGen.getNextULong(), twUser, location, sendTime, message.getReferredTopics(), message,
                    btreeExtraFieldArray, DUMMY_SIZE_ADJUSTER);
            return twMessage;
        }

    }

    public static class LocationGeneratorFromOpenStreetMapData {
        /**
         * the source of gps data:
         * https://blog.openstreetmap.org/2012/04/01/bulk-gps-point-data/
         */
        private String openStreetMapFilePath;
        private long sampleInterval;
        private long lineCount = 0;
        private BufferedReader br;
        private String line;
        private String strPoints[] = null;
        private StringBuilder sb = new StringBuilder();
        private Point point = new Point();
        private float[] floatPoint = new float[2];

        public void intialize(String openStreetMapFilePath, int sampleInterval) {
            this.openStreetMapFilePath = openStreetMapFilePath;
            this.sampleInterval = sampleInterval;
            try {
                br = new BufferedReader(new FileReader(openStreetMapFilePath));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
        }

        public Point getNextPoint() {
            try {
                while (true) {
                    if ((line = br.readLine()) == null) {
                        br = new BufferedReader(new FileReader(openStreetMapFilePath));
                        line = br.readLine(); //can't be null
                    }
                    if (lineCount++ % sampleInterval != 0) {
                        continue;
                    }
                    sb.setLength(0);
                    strPoints = line.split(",");
                    if (strPoints.length != 2) {
                        //ignore invalid point
                        continue;
                    } else {
                        break;
                    }
                }
                if (line == null) {
                    //roll over the data from the same file.
                    br.close();
                    br = null;
                    lineCount = 0;
                    br = new BufferedReader(new FileReader(openStreetMapFilePath));
                    while ((line = br.readLine()) != null) {
                        if (lineCount++ % sampleInterval != 0) {
                            continue;
                        }
                        sb.setLength(0);
                        strPoints = line.split(",");
                        if (strPoints.length != 2) {
                            //ignore invalid point
                            continue;
                        } else {
                            break;
                        }
                    }
                }
                floatPoint[0] = Float.parseFloat(strPoints[0]) / 10000000; //latitude (y value)
                floatPoint[1] = Float.parseFloat(strPoints[1]) / 10000000; //longitude (x value)
                point.reset(floatPoint[1], floatPoint[0]);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
            return point;
        }

        @Override
        public void finalize() {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IllegalStateException(e);
                }
            }
        }
    }

}