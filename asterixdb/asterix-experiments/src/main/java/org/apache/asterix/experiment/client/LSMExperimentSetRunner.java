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

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.builder.BaseExperimentBuilder;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.reflections.Reflections;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.TypeElementsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

public class LSMExperimentSetRunner {

    private static final Logger LOGGER = Logger.getLogger(LSMExperimentSetRunner.class.getName());

    public static void main(String[] args) throws Exception {
        LSMExperimentSetRunnerConfig config =
                new LSMStatsExperimentSetRunnerConfig(String.valueOf(System.currentTimeMillis()));

        //        LogManager.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
        config = DriverUtils.getCmdParams(args, config);
        CloseableHttpClient httpClient =
                HttpClients.custom().setConnectionManager(new PoolingHttpClientConnectionManager()).setMaxConnTotal(100)
                        .setMaxConnPerRoute(50)
                        .setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(0).setSoKeepAlive(true).build())
                        .setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(0)
                                .setConnectionRequestTimeout(0).setSocketTimeout(0).build())
                        .build();

        String pkg = "org.apache.asterix.experiment.builder.stats.suite";
        Reflections reflections =
                new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage(pkg))
                        .filterInputsBy(new FilterBuilder().includePackage(pkg))
                        .setScanners(new TypeElementsScanner().publicOnly(), new MethodParameterScanner()));
        Map<String, BaseExperimentBuilder> nameMap = new TreeMap<>();
        for (Constructor c : reflections.getConstructorsMatchParams(LSMExperimentSetRunnerConfig.class,
                CloseableHttpClient.class)) {
            BaseExperimentBuilder b = (BaseExperimentBuilder) c.newInstance(config, httpClient);
            nameMap.put(b.getName(), b);
        }

        Pattern p = config.getRegex() == null ? null : Pattern.compile(config.getRegex());

        SequentialActionList exps = new SequentialActionList("executed experiments");
        for (Map.Entry<String, BaseExperimentBuilder> e : nameMap.entrySet()) {
            if (p == null || p.matcher(e.getKey()).matches()) {
                exps.addLast(e.getValue().build());
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Added " + e.getKey() + " to run list...");
                }
            }
        }
        exps.perform();
        httpClient.close();
    }
}
