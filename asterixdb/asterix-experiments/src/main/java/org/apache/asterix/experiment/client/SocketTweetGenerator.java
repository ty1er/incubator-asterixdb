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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.external.generator.TweetGenerator;
import org.apache.commons.lang3.tuple.Pair;

public class SocketTweetGenerator {

    private final ExecutorService threadPool;

    private final int partitionRangeStart;

    private final int dataGenDuration;

    private final int queryGenDuration;

    private final int nDataIntervals;

    private final String orchHost;

    private final int orchPort;

    private final List<Pair<String, Integer>> receiverAddresses;

    private final int recordCountPerBatchDuringIngestionOnly;
    private final int recordCountPerBatchDuringQuery;
    private final long dataGenSleepTimeDuringIngestionOnly;
    private final long dataGenSleepTimeDuringQuery;
    private final Map<String, String> properties;
    private final Mode mode;
    private final int partitionsNum;
    private final LSMExperimentSetRunnerConfig.WorkloadType workloadType;
    private final double deletesPercentage;
    private final double updatesPercentage;
    private final long seed;
    private final int dataGenCount;
    private final int upperBound;
    private final int lowerBound;

    private enum Mode {
        TIME,
        DATA
    }

    public SocketTweetGenerator(SocketTweetGeneratorStatisticsConfig config, Map<String, String> propeties) {
        this.properties = propeties;
        threadPool = Executors.newCachedThreadPool(new ThreadFactory() {

            private final AtomicInteger count = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                int tid = count.getAndIncrement();
                Thread t = new Thread(r, "SocketDataGeneratorThread: " + tid);
                t.setDaemon(true);
                return t;
            }
        });

        partitionRangeStart = config.getPartitionId();
        partitionsNum = config.getPartitionsNum();
        dataGenDuration = config.getDatagenDuration();
        dataGenCount = config.getDatagenCount();
        queryGenDuration = config.getQueryGenDuration();
        nDataIntervals = config.getNIntervals();
        orchHost = config.getOrchestratorHost();
        orchPort = config.getOrchestratorPort();
        receiverAddresses = config.getAddresses();
        upperBound = config.getUpperBound();
        lowerBound = config.getLowerBound();
        mode = dataGenCount > 0 ? Mode.DATA : Mode.TIME;
        recordCountPerBatchDuringIngestionOnly = config.getRecordCountPerBatchDuringIngestionOnly();
        recordCountPerBatchDuringQuery = config.getRecordCountPerBatchDuringQuery();
        dataGenSleepTimeDuringIngestionOnly = config.getDataGenSleepTimeDuringIngestionOnly();
        dataGenSleepTimeDuringQuery = config.getDataGenSleepTimeDuringQuery();
        deletesPercentage = config.getDeletesPercentage();
        updatesPercentage = config.getUpdatesPercentage();
        this.workloadType = config.getWorkloadType();
        seed = config.getSeed();
    }

    public void start() throws Exception {
        final Semaphore sem = new Semaphore((receiverAddresses.size() - 1) * -1);
        int i = 0;
        for (Pair<String, Integer> address : receiverAddresses) {
            threadPool.submit(new SocketDataGenerator(mode, sem, address.getLeft(), address.getRight(),
                    i + partitionRangeStart, partitionsNum, dataGenDuration, dataGenCount, upperBound, lowerBound,
                    queryGenDuration, nDataIntervals, orchHost, orchPort, recordCountPerBatchDuringIngestionOnly,
                    recordCountPerBatchDuringQuery, dataGenSleepTimeDuringIngestionOnly, dataGenSleepTimeDuringQuery,
                    workloadType, deletesPercentage, updatesPercentage, seed, properties));
            ++i;
        }
        sem.acquire();
    }

    public static class SocketDataGenerator implements Runnable {

        private static final Logger LOGGER = Logger.getLogger(SocketDataGenerator.class.getName());

        private final Mode m;
        private final Semaphore sem;
        private final String host;
        private final int port;
        private final int partition;
        private final int partitionsNum;
        private final int dataGenDuration;
        private final int queryGenDuration;
        private final int nDataIntervals;
        private final Map<String, String> properties;
        private final String orchHost;
        private final int orchPort;

        private int currentInterval;
        private int nextStopInterval;
        private final long dataSizeInterval;
        private final int recordCountPerBatchDuringIngestionOnly;
        private final int recordCountPerBatchDuringQuery;
        private final long dataGenSleepTimeDuringIngestionOnly;
        private final long dataGenSleepTimeDuringQuery;
        private final LSMExperimentSetRunnerConfig.WorkloadType workloadType;
        private final double deletesPercentage;
        private final double updatesPercentage;
        private final long seed;
        private final int upperBound;
        private final int lowerBound;

        public SocketDataGenerator(Mode m, Semaphore sem, String host, int port, int partition, int partitionsNum,
                int dataGenDuration, int dataGenCount, int upperBound, int lowerBound, int queryGenDuration,
                int nDataIntervals, String orchHost, int orchPort, int recordCountPerBatchDuringIngestionOnly,
                int recordCountPerBatchDuringQuery, long dataGenSleepTimeDuringIngestionOnly,
                long dataGenSleepTimeDuringQuery, LSMExperimentSetRunnerConfig.WorkloadType workloadType,
                double deletesPercentage, double updatesPercentage, long seed, Map<String, String> properties) {
            this.m = m;
            this.sem = sem;
            this.host = host;
            this.port = port;
            this.partition = partition;
            this.partitionsNum = partitionsNum;
            this.dataGenDuration = dataGenDuration;
            this.upperBound = upperBound;
            this.lowerBound = lowerBound;
            this.queryGenDuration = queryGenDuration;
            this.nDataIntervals = nDataIntervals;
            this.properties = properties;
            currentInterval = 0;
            this.dataSizeInterval = dataGenCount / (nDataIntervals * partitionsNum);
            this.nextStopInterval = dataGenCount / (nDataIntervals * partitionsNum);
            this.orchHost = orchHost;
            this.orchPort = orchPort;
            this.recordCountPerBatchDuringIngestionOnly = recordCountPerBatchDuringIngestionOnly;
            this.recordCountPerBatchDuringQuery = recordCountPerBatchDuringQuery;
            this.dataGenSleepTimeDuringIngestionOnly = dataGenSleepTimeDuringIngestionOnly;
            this.dataGenSleepTimeDuringQuery = dataGenSleepTimeDuringQuery;
            this.workloadType = workloadType;
            //adjust percentages
            if (nDataIntervals > 1) {
                updatesPercentage *= (double) nDataIntervals / (nDataIntervals - 1);
                deletesPercentage *= (double) nDataIntervals / (nDataIntervals - 1);
            }
            this.deletesPercentage = deletesPercentage;
            this.updatesPercentage = updatesPercentage;
            this.seed = seed;
        }

        @Override
        public void run() {
            LOGGER.info("\nDataGen[" + partition + "] running with the following parameters: \n" + "dataGenDuration : "
                    + dataGenDuration + "\n" + "queryGenDuration : " + queryGenDuration + "\n" + "nDataIntervals : "
                    + nDataIntervals + "\n" + "dataSizeInterval : " + dataSizeInterval + "\n"
                    + "recordCountPerBatchDuringIngestionOnly : " + recordCountPerBatchDuringIngestionOnly + "\n"
                    + "recordCountPerBatchDuringQuery : " + recordCountPerBatchDuringQuery + "\n"
                    + "dataGenSleepTimeDuringIngestionOnly : " + dataGenSleepTimeDuringIngestionOnly + "\n"
                    + "dataGenSleepTimeDuringQuery : " + dataGenSleepTimeDuringQuery);

            while (true) {
                try (Socket s = new Socket(host, port)) {
                    try {
                        Socket orchSocket = null;
                        if (m == Mode.DATA && orchHost != null) {
                            orchSocket = new Socket(orchHost, orchPort);
                        }
                        TweetGenerator tg = null;
                        String durationVal = m == Mode.TIME ? String.valueOf(dataGenDuration) : "0";
                        properties.put(TweetGenerator.KEY_DURATION, String.valueOf(durationVal));
                        properties.put(TweetGenerator.KEY_GUID_SEED, String.valueOf(seed));
                        properties.put(TweetGenerator.KEY_PERCENTAGE_UPDATES, String.valueOf(updatesPercentage));
                        properties.put(TweetGenerator.KEY_PERCENTAGE_DELETES, String.valueOf(deletesPercentage));
                        properties.put(TweetGenerator.KEY_CHANGE_FEED,
                                String.valueOf(workloadType == LSMExperimentSetRunnerConfig.WorkloadType.Varying));
                        properties.put(TweetGenerator.KEY_GUID_INCREMENT, String.valueOf(partitionsNum));
                        tg = new TweetGenerator(properties, partition,
                                GeneratorFactory.getDataGenerator(workloadType, properties, upperBound, lowerBound,
                                        partition),
                                GeneratorFactory.getIDGenerator(workloadType, properties, partition));
                        tg.registerSubscriber(s.getOutputStream());
                        long startTS = System.currentTimeMillis();
                        long prevTS = startTS;
                        long curTS = startTS;
                        int round = 0;
                        int numTweetsToGenerate = 0;
                        if (m == Mode.DATA) {
                            numTweetsToGenerate = Math.min(recordCountPerBatchDuringIngestionOnly,
                                    nextStopInterval - tg.getNumFlushedTweets() - tg.getFrameTweetCount());
                        } else if (m == Mode.TIME) {
                            numTweetsToGenerate = recordCountPerBatchDuringIngestionOnly;
                        }
                        try {
                            sendOrchestratorMessage(orchSocket, OrchestratorDGProtocol.START);
                            while (tg.generateNextBatch(numTweetsToGenerate)) {
                                if (m == Mode.DATA) {
                                    if (tg.getNumFlushedTweets() + tg.getFrameTweetCount() >= nextStopInterval) {
                                        tg.forceFlush();
                                        //TODO stop/resume option
                                        if (orchSocket != null) {
                                            sendOrchestratorMessage(orchSocket, OrchestratorDGProtocol.REACHED);
                                        }

                                        // update intervals
                                        // TODO give options: exponential/linear interval
                                        nextStopInterval += dataSizeInterval;
                                        if (++currentInterval >= nDataIntervals) {
                                            break;
                                        }

                                        if (orchSocket != null) {
                                            receiveResume(orchSocket);
                                        }
                                    }
                                }
                                curTS = System.currentTimeMillis();
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    round++;
                                    if ((round * recordCountPerBatchDuringIngestionOnly) % 100000 == 0) {
                                        LOGGER.info(
                                                "DataGen[" + partition + "][During ingestion only][TimeToInsert100000] "
                                                        + (curTS - prevTS) + " in milliseconds");
                                        round = 0;
                                        prevTS = curTS;
                                    }
                                }
                                //to prevent congestion in feed pipe line.
                                if (dataGenSleepTimeDuringIngestionOnly > 0) {
                                    Thread.sleep(dataGenSleepTimeDuringIngestionOnly);
                                }
                            }

                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("DataGen[" + partition
                                        + "][During ingestion only][InsertCount] Num tweets flushed = "
                                        + tg.getNumFlushedTweets() + " in "
                                        + ((System.currentTimeMillis() - startTS) / 1000) + " seconds from "
                                        + InetAddress.getLocalHost() + " to " + host + ":" + port);
                            }

                            if (orchSocket != null) {
                                //send stop message to orchestrator server
                                sendOrchestratorMessage(orchSocket, OrchestratorDGProtocol.STOPPED);
                            }
                            break;
                        } catch (Throwable t) {
                            if (LOGGER.isLoggable(Level.SEVERE))
                                LOGGER.log(Level.SEVERE, "Error during socket tweet generation", t);
                        } finally {
                            if (orchSocket != null) {
                                orchSocket.close();
                            }
                            if (LOGGER.isLoggable(Level.INFO)) {
                                long ingestionSeconds =
                                        dataGenDuration > 0 ? dataGenDuration : System.currentTimeMillis() - startTS;
                                LOGGER.info("Num tweets flushed = " + tg.getNumFlushedTweets() + " in "
                                        + ingestionSeconds + " seconds from " + InetAddress.getLocalHost() + " to "
                                        + host + ":" + port);
                            }
                        }
                    } catch (Exception t) {
                        if (LOGGER.isLoggable(Level.SEVERE))
                            LOGGER.log(Level.SEVERE, "Error during socket tweet generation", t);
                    } finally {
                        s.close();
                    }
                } catch (IOException t) {
                    if (LOGGER.isLoggable(Level.SEVERE))
                        LOGGER.log(Level.SEVERE, "Error connecting to " + host + ":" + port, t);
                } finally {
                    sem.release();
                }
                if (LOGGER.isLoggable(Level.SEVERE))
                    LOGGER.log(Level.SEVERE, "Reconnecting...");
            }
        }

        private void sendOrchestratorMessage(Socket s, OrchestratorDGProtocol msgType) throws IOException {
            new DataOutputStream(s.getOutputStream()).writeInt(msgType.ordinal());
            s.getOutputStream().flush();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Sent " + msgType + " to " + s.getRemoteSocketAddress());
            }
        }

        private void receiveResume(Socket s) throws IOException {
            int msg = new DataInputStream(s.getInputStream()).readInt();
            OrchestratorDGProtocol msgType = OrchestratorDGProtocol.values()[msg];
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Received " + msgType + " from " + s.getRemoteSocketAddress());
            }
            if (msgType != OrchestratorDGProtocol.RESUME) {
                throw new IllegalStateException("Unknown protocol message received: " + msgType);
            }
        }

    }

    private static class CircularByteArrayOutputStream extends OutputStream {

        private final byte[] buf;

        private int index;

        public CircularByteArrayOutputStream() {
            buf = new byte[32 * 1024];
            index = 0;
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return;
            }

            int remain = len;
            int remainOff = off;
            while (remain > 0) {
                int avail = buf.length - index;
                System.arraycopy(b, remainOff, buf, index, avail);
                remainOff += avail;
                remain -= avail;
                index = (index + avail) % buf.length;
            }
        }

        @Override
        public void write(int b) throws IOException {
            buf[index] = (byte) b;
            index = (index + 1) % buf.length;
        }

    }
}
