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

import static org.apache.asterix.experiment.client.GeneratorFactory.KEY_MIN_SPREAD;
import static org.apache.asterix.experiment.client.GeneratorFactory.KEY_ZIPF_SKEW;
import static org.apache.asterix.external.generator.TweetGenerator.KEY_GUID_SEED;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig.WorkloadType;
import org.apache.asterix.external.generator.TweetGenerator;

public class FileGeneratorDriver {
    public static void main(String[] args) throws Exception {
        FileTweetGeneratorStatisticsConfig config = null;
        if (args.length > 2 && args[0].equals("-wt")) {
            WorkloadType type = WorkloadType.valueOf(args[1]);
            if (type == null) {
                throw new IllegalArgumentException("Unspecified tweet generator type");
            }
            switch (type) {
                case InsertOnly:
                case Varying:
                    config = new FileTweetGeneratorStatisticsConfig();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown tweet generator type:" + type);
            }
        }
        FileTweetGeneratorStatisticsConfig statsConfig = DriverUtils.getCmdParams(args, config);

        Map<String, String> configuration = new HashMap<>();
        configuration.put(TweetGenerator.KEY_GUID_INCREMENT, String.valueOf(statsConfig.getPartitionsNum()));
        List<String> genFilePaths = null;
        configuration.put(KEY_ZIPF_SKEW, statsConfig.getSkew().toString());
        configuration.put(KEY_MIN_SPREAD, Integer.toString(RangeGenerator.MIN_SPREAD));
        configuration.put(KEY_GUID_SEED, Long.toString(statsConfig.getSeed()));
        configuration.put(TweetGenerator.KEY_PERCENTAGE_UPDATES, String.valueOf(statsConfig.getUpdatesPercentage()));
        configuration.put(TweetGenerator.KEY_PERCENTAGE_DELETES, String.valueOf(statsConfig.getDeletesPercentage()));
        configuration.put(TweetGenerator.KEY_CHANGE_FEED,
                String.valueOf(statsConfig.getWorkloadType() == LSMExperimentSetRunnerConfig.WorkloadType.Varying));
        genFilePaths = statsConfig.getPaths();

        //get record count to be generated per partition
        final int recordCount = statsConfig.getDatagenCount() / statsConfig.getPartitionsNum();
        new FileTweetGenerator((Integer i) -> new TweetGenerator(configuration, i + statsConfig.getPartitionId(),
                GeneratorFactory.getDataGenerator(statsConfig.getWorkloadType(), configuration,
                        statsConfig.getUpperBound(), statsConfig.getLowerBound(), i + statsConfig.getPartitionId()),
                GeneratorFactory.getIDGenerator(statsConfig.getWorkloadType(), configuration,
                        i + statsConfig.getPartitionId())),
                recordCount, genFilePaths).start();
    }

    static class FileTweetGenerator {
        private final Function<Integer, TweetGenerator> generatorFactory;
        private final int recordCount;
        private final List<String> paths;
        private final ExecutorService threadPool;

        public FileTweetGenerator(Function<Integer, TweetGenerator> generatorFactory, int recordCount,
                List<String> paths) {
            this.generatorFactory = generatorFactory;
            this.recordCount = recordCount;
            this.paths = paths;
            threadPool = Executors.newCachedThreadPool(new ThreadFactory() {

                private final AtomicInteger count = new AtomicInteger();

                @Override
                public Thread newThread(Runnable r) {
                    int tid = count.getAndIncrement();
                    Thread t = new Thread(r, "FileDataGeneratorThread: " + tid);
                    t.setDaemon(true);
                    return t;
                }
            });
        }

        public void start() throws Exception {
            final Semaphore sem = new Semaphore((paths.size() - 1) * -1);
            int i = 0;
            for (String filePath : paths) {
                threadPool
                        .submit(new FileTweetGeneratorThread(sem, generatorFactory.apply(i++), recordCount, filePath));
            }
            sem.acquire();
        }
    }

    static class FileTweetGeneratorThread implements Runnable {

        private final Semaphore sem;
        private final TweetGenerator generator;
        private final int recordCount;
        private final String filePath;

        public FileTweetGeneratorThread(Semaphore sem, TweetGenerator generator, int recordCount, String filePath) {
            this.sem = sem;
            this.generator = generator;
            this.recordCount = recordCount;
            this.filePath = filePath;
        }

        @Override
        public void run() {
            //prepare timer
            long startTS = System.currentTimeMillis();

            OutputStream fos = null;
            try {
                //prepare output file
                File file = new File(filePath);
                if (file.exists()) {
                    file.delete();
                }
                file.createNewFile();
                fos = new BufferedOutputStream(new FileOutputStream(file));
                generator.registerSubscriber(fos);

                generator.generateNextBatch(recordCount);
                if (generator.getNumFlushedTweets() < recordCount && generator.getFrameTweetCount() > 0) {
                    generator.forceFlush();
                }

                System.out.println("Done: generated " + recordCount + " records in "
                        + ((System.currentTimeMillis() - startTS) / 1000) + " in seconds!");
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                sem.release();
                try {
                    if (fos != null) {
                        fos.flush();
                        fos.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
