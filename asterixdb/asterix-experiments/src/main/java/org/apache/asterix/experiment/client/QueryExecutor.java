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

import java.io.File;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.derived.RunQueryFileAction;
import org.apache.commons.io.FileUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class QueryExecutor {
    private static final Logger LOGGER = Logger.getLogger(QueryExecutor.class.getName());

    public static void main(String[] args) {
        QueryExecutorConfig config = DriverUtils.getCmdParams(args, new QueryExecutorConfig());
        PoolingHttpClientConnectionManager poolingCCM = new PoolingHttpClientConnectionManager();
        poolingCCM.setDefaultMaxPerRoute(10);
        CloseableHttpClient httpClient =
                HttpClients.custom().setConnectionManager(poolingCCM).setMaxConnPerRoute(10).build();
        try {
            RunQueryFileAction runAction =
                    new RunQueryFileAction(httpClient, config.getHost(), config.getPort(), Paths.get(config.getQuery()),
                            FileUtils.openOutputStream(new File(config.getOutput())),
                            config.getOutputType().getContentType());
            runAction.doPerform();
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.SEVERE))
                LOGGER.log(Level.SEVERE, "Error while executing query " + config.getQuery(), e);
        }
    }
}
