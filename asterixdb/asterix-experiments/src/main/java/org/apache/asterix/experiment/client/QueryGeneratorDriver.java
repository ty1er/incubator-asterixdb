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

import java.io.FileOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig.WorkloadType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.hyracks.api.util.ExperimentProfilerUtils;

public class QueryGeneratorDriver {

    public static void main(String[] args) throws Exception {
        final Semaphore sem = new Semaphore(0);
        QueryGeneratorConfig config = null;
        if (args.length > 2 && args[0].equals("-wt")) {
            WorkloadType type = WorkloadType.valueOf(args[1]);
            if (type == null)
                throw new IllegalArgumentException("Unspecified query generator type");
            switch (type) {
                case InsertOnly:
                case Varying:
                    config = new StatisticsQueryGeneratorConfig();
                    break;
                case WorldCup:
                    config = new WorldCupQueryGeneratorConfig();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown query generator type:" + type);
            }
            config = DriverUtils.getCmdParams(args, config);

            ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactory() {

                private final AtomicInteger count = new AtomicInteger();

                @Override
                public Thread newThread(Runnable r) {
                    int tid = count.getAndIncrement();
                    Thread t = new Thread(r, "DataGeneratorThread: " + tid);
                    t.setDaemon(true);
                    return t;
                }
            });

            //number of parallel generator threads
            int genThreads = 1;
            FileOutputStream outputFos = null;
            CloseableHttpClient httpClient =
                    HttpClients.custom().setConnectionManager(new PoolingHttpClientConnectionManager()).build();
            try {
                outputFos = ExperimentProfilerUtils.openOutputFile(config.getOutput());

                for (int i = 0; i < genThreads; i++) {
                    QueryGenerator qGen =
                            GeneratorFactory.getQueryGenerator(type, sem, genThreads, config, outputFos, httpClient);
                    threadPool.submit(qGen, null);
                }
                sem.acquire(genThreads);
            } catch (Exception e) {
                e.printStackTrace();
                outputFos.write("Error during query generation\n".getBytes());
            } finally {
                if (outputFos != null) {
                    ExperimentProfilerUtils.closeOutputFile(outputFos);
                }
            }
        }
    }
}
