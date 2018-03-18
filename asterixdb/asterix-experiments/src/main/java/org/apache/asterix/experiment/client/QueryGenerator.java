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

import org.apache.http.impl.client.CloseableHttpClient;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class QueryGenerator implements Runnable {
    protected final Semaphore sem;
    protected final String restHost;
    protected final int restPort;
    protected final int partition;
    protected final int queryDuration;
    protected long queryCount;
    protected final CloseableHttpClient httpClient;
    protected FileOutputStream outputFos;

    private static final Logger LOGGER = Logger.getLogger(QueryGenerator.class.getName());

    public QueryGenerator(Semaphore sem, QueryGeneratorConfig config, int threadsNum, FileOutputStream outputFos,
            CloseableHttpClient httpClient) {
        this.sem = sem;
        this.partition = config.getPartitionNum();
        this.queryCount = config.getQueryCount() / (threadsNum * partition);
        this.queryDuration = config.getDuration() * 1000;
        this.restHost = config.getRESTHost();
        this.restPort = config.getRESTPort();
        this.httpClient = httpClient;
        this.outputFos = outputFos;
    }

    @Override
    public void run() {
        LOGGER.info("\nQueryGen[" + partition + "] running with the following parameters: \n" + "queryGenDuration : "
                + queryDuration + " queryGenCount:" + queryCount);

        try {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("QueryGen[" + partition + "] starts sending queries...");
            }
            //send queries during query duration
            long startTS = System.currentTimeMillis();
            long prevTS = startTS;
            long curTS = startTS;
            long genertedQueries = 0;
            while (curTS - startTS < queryDuration || genertedQueries < queryCount) {
                sendQuery();
                genertedQueries++;
                curTS = System.currentTimeMillis();
                if (LOGGER.isLoggable(Level.INFO) && genertedQueries % 100 == 0) {
                    LOGGER.info("QueryGen[" + partition + "][TimeToQuery100] " + (curTS - prevTS)
                            + " in milliseconds");
                    prevTS = curTS;
                }
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("QueryGen[" + partition + "][QueryCount] " + genertedQueries + " in "
                        + (queryDuration / 1000) + " seconds");
            }
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "Error connecting to rest API server " + restHost + ":" + restPort, t);
            throw new RuntimeException(t);
        } finally {
            sem.release();
        }
    }

    protected abstract void sendQuery() throws IOException;
}
