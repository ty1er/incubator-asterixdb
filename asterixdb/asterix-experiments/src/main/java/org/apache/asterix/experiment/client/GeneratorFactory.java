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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.asterix.experiment.client.DataGeneratorStatistics.StatisticsInitializationInfo;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig.WorkloadType;
import org.apache.asterix.external.generator.DataGenerator;
import org.apache.asterix.external.generator.TweetGenerator;
import org.apache.asterix.external.util.IDGenerator;
import org.apache.asterix.external.util.SimplePartitionIDGenerator;
import org.apache.http.impl.client.CloseableHttpClient;

public class GeneratorFactory {

    public static final String KEY_OPENSTREETMAP_FILEPATH = "open-street-map-filepath";
    public static final String KEY_LOCATION_SAMPLE_INTERVAL = "location-sample-interval";
    public static final String KEY_ZIPF_SKEW = "zipf-skew";
    public static final String KEY_MIN_SPREAD = "min-spread";

    private static final int DEFAULT_DURATION = 60; //seconds
    private static final long DEFAULT_GUID_SEED = 0l;
    private static final int DEFAULT_SAMPLE_INTERVAL = 1;
    private static final double DEFAULT_ZIPF_SKEW = 1.5;
    private static final int DEFAULT_MIN_SPREAD = 0;

    public static enum GeneratorType {
        Basic,
        Perf,
        Spatial,
        Statistics,
        WorldCup
    }

    public static String getDatagenCmd(LSMExperimentSetRunnerConfig config, int partition, int partitionsNum,
            String genItems) {
        StringBuilder sb = new StringBuilder();
        Path binaryPath = Paths.get(config.getLocalExperimentRoot()).resolve("bin");
        switch (config.getIngestType()) {
            case SocketFeed:
                binaryPath = binaryPath.resolve("socketDatagenRunner");
                break;
            case FileLoad:
            case FileFeed:
                binaryPath = binaryPath.resolve("fileDatagenRunner");
                break;
        }
        sb.append("JAVA_OPTS=\"");
        sb.append("-Xmx1G ");
        //sb.append("-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=0,suspend=y ");
        sb.append("\" ");
        sb.append("JAVA_HOME=").append(config.getJavaHome()).append(" ").append(binaryPath).append(" -wt ")
                .append(config.getWorkloadType()).append(" -pid ").append(partition).append(" -pn ")
                .append(partitionsNum).append(" -s ").append(config.getSeed());
        switch (config.getIngestType()) {
            case SocketFeed:
                long tweetsPerBatchNum = config.getRecordCountPerBatchDuringIngestionOnly();
                if (config.getDatagenCount() > 0) {
                    tweetsPerBatchNum = config.getDatagenCount() / config.getNIntervals();
                }
                sb.append(" -ni ").append(config.getNIntervals()).append(" -rcbi ").append(tweetsPerBatchNum)
                        .append(" -rcbq ").append(config.getRecordCountPerBatchDuringQuery()).append(" -dsti ")
                        .append(config.getDataGenSleepTimeDuringIngestionOnly()).append(" -dstq ")
                        .append(config.getDataGenSleepTimeDuringQuery()).append(" -d ")
                        .append(config.getDatagenDuration()).append(" -dc ").append(config.getDatagenCount())
                        .append(" -oh ").append(config.getOrchestratorHost()).append(" -op ")
                        .append(config.getOrchestratorPort());
                break;
            case FileLoad:
            case FileFeed:
                sb.append(" -dc ").append(config.getDatagenCount());
                break;
        }
        LSMStatsExperimentSetRunnerConfig statsConfig = (LSMStatsExperimentSetRunnerConfig) config;
        sb.append(" -z ").append(statsConfig.getSkew()).append(" -ub ").append(statsConfig.getUpperBound())
                .append(" -lb ").append(statsConfig.getLowerBound()).append(" -pu ")
                .append(statsConfig.getUpdatesPercentage()).append(" -pd ").append(statsConfig.getDeletesPercentage());
        sb.append(" ").append(genItems);
        // launch in background mode
        //.append("&");
        return sb.toString();
    }

    public static String getQueryGenCmd(LSMExperimentSetRunnerConfig config, int partition, int partitionsNum) {
        StringBuilder sb = new StringBuilder();
        Path binaryPath = Paths.get(config.getLocalExperimentRoot()).resolve("bin").resolve("querygenrunner");
        LSMStatsExperimentSetRunnerConfig statsConfig = (LSMStatsExperimentSetRunnerConfig) config;
        sb.append(binaryPath).append(" -wt ").append(statsConfig.getWorkloadType()).append(" -rh ")
                .append(statsConfig.getRESTHost()).append(" -rp ").append(statsConfig.getRESTPort()).append(" -o ")
                .append(statsConfig.getQgenOutputFilePath()).append(" -qc ").append(statsConfig.getQueryCount())
                .append(" -p ").append(partitionsNum).append(" -pr ").append(statsConfig.getRangePercent())
                .append(" -ub ").append(statsConfig.getUpperBound()).append(" -lb ").append(statsConfig.getLowerBound())
                .append(" -s ").append(statsConfig.getSeed() + partition);

        switch (statsConfig.getWorkloadType()) {
            case InsertOnly:
            case Varying:
                sb.append(" -rt ").append(statsConfig.getRangeType()).append(" -rl ")
                        .append(statsConfig.getRangeLength()).append(" -z ").append(statsConfig.getSkew());
                break;
            case WorldCup:
                sb.append(" -rt ").append(StatisticsRangeType.Percentage.toString());
                break;
        }
        return sb.toString();
    }

    public static DataGenerator getDataGenerator(WorkloadType type, Map<String, String> configuration, int domainMax,
            int domainMin, int partition) {
        switch (type) {
            case InsertOnly:
            case Varying:
                int partitionIncrement = getIDGeneratorIncrement(configuration);
                return new DataGeneratorStatistics(new StatisticsInitializationInfo(getSkew(configuration), domainMax,
                        domainMin, getIDGeneratorSeed(configuration), partition, partitionIncrement,
                        getMinSpread(configuration)));
            default:
                throw new IllegalArgumentException("Illegal data generator type:" + type);
        }
    }

    public static IDGenerator getIDGenerator(WorkloadType type, Map<String, String> configuration, int partition) {
        switch (type) {
            case InsertOnly:
            case Varying:
                return new SimplePartitionIDGenerator(partition, getIDGeneratorIncrement(configuration));
            default:
                throw new IllegalArgumentException("Illegal data generator type:" + type);
        }
    }

    private static int getMinSpread(Map<String, String> configuration) {
        return configuration.get(KEY_MIN_SPREAD) != null ? Integer.parseInt(configuration.get(KEY_MIN_SPREAD))
                : DEFAULT_MIN_SPREAD;
    }

    private static double getSkew(Map<String, String> configuration) {
        String skew = configuration.get(KEY_ZIPF_SKEW);
        return skew != null ? Double.parseDouble(skew) : DEFAULT_ZIPF_SKEW;
    }

    public static long getIDGeneratorSeed(Map<String, String> configuration) {
        return configuration.get(TweetGenerator.KEY_GUID_SEED) != null
                ? Long.parseLong(configuration.get(TweetGenerator.KEY_GUID_SEED)) : DEFAULT_GUID_SEED;
    }

    public static int getIDGeneratorIncrement(Map<String, String> configuration) {
        return configuration.get(TweetGenerator.KEY_GUID_INCREMENT) != null
                ? Integer.parseInt(configuration.get(TweetGenerator.KEY_GUID_INCREMENT)) : 1;
    }

    public static QueryGenerator getQueryGenerator(WorkloadType type, Semaphore s, int threadsNum,
            QueryGeneratorConfig config, FileOutputStream outputFos, CloseableHttpClient httpClient) {
        switch (type) {
            case InsertOnly:
            case Varying:
                return new StatisticsQueryGenerator(s, (StatisticsQueryGeneratorConfig) config, threadsNum, outputFos,
                        httpClient);
            case WorldCup:
                return new WorldCupQueryGenerator(s, (WorldCupQueryGeneratorConfig) config, threadsNum, outputFos,
                        httpClient);
            default:
                throw new IllegalArgumentException("Illegal query generator type:" + type);
        }
    }
}
