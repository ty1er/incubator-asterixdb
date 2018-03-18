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

import org.kohsuke.args4j.Option;

public class LSMExperimentSetRunnerConfig {

    private final String logDirSuffix;

    public LSMExperimentSetRunnerConfig(String logDirSuffix) {
        this.logDirSuffix = logDirSuffix;
    }

    public String getLogDirSuffix() {
        return logDirSuffix;
    }

    @Option(name = "-r", aliases = "--runs-num", usage = "Number of runs for each experiment")
    private int expRunsNum = 1;

    public int getExpRunsNum() {
        return expRunsNum;
    }

    @Option(name = "-rh", aliases = "--rest-host", usage = "Asterix REST API host address", required = true, metaVar = "HOST")
    private String restHost;

    public String getRESTHost() {
        return restHost;
    }

    @Option(name = "-rp", aliases = "--rest-port", usage = "Asterix REST API port", required = true, metaVar = "PORT")
    private int restPort;

    public int getRESTPort() {
        return restPort;
    }

    @Option(name = "-oh", aliases = "--orchestrator-host", usage = "The host address of THIS orchestrator")
    private String orchHost;

    public String getOrchestratorHost() {
        return orchHost;
    }

    @Option(name = "-op", aliases = "--orchestrator-port", usage = "The port to be used for the orchestrator server of THIS orchestrator")
    private int orchPort;

    public int getOrchestratorPort() {
        return orchPort;
    }

    @Option(name = "-mh", aliases = "--managix-home", usage = "Path to MANAGIX_HOME directory", required = true, metaVar = "MGXHOME")
    private String managixHome;

    public String getManagixHome() {
        return managixHome;
    }

    @Option(name = "-jh", aliases = "--java-home", usage = "Path to JAVA_HOME directory", required = true, metaVar = "JAVAHOME")
    private String javaHome;

    public String getJavaHome() {
        return javaHome;
    }

    @Option(name = "-ler", aliases = "--local-experiment-root", usage = "Path to the local LSM experiment root directory", required = true, metaVar = "LOCALEXPROOT")
    private String localExperimentRoot;

    public String getLocalExperimentRoot() {
        return localExperimentRoot;
    }

    @Option(name = "-u", aliases = "--username", usage = "Username to use for SSH/SCP", required = true, metaVar = "UNAME")
    private String username;

    public String getUsername() {
        return username;
    }

    @Option(name = "-k", aliases = "--key", usage = "SSH key location", metaVar = "SSHKEY")
    private String sshKeyLocation;

    public String getSSHKeyLocation() {
        return sshKeyLocation;
    }

    public enum WorkloadType {
        InsertOnly("insert"),
        Varying("change"),
        WorldCup("worldcup");

        private final String dir;

        WorkloadType(String dir) {
            this.dir = dir;
        }

        public String getDir() {
            return dir;
        }
    }

    @Option(name = "-wt", aliases = "--workload-type", usage = "Type of data workload ({InsertOnly|Varying|WorldCup})", required = true)
    private WorkloadType workloadType;

    public WorkloadType getWorkloadType() {
        return workloadType;
    }

    static public enum IngestionType {
        FileLoad,
        SocketFeed,
        FileFeed
    }

    @Option(name = "-dt", aliases = "--datagen-type", usage = "Type of data ingest ({FileLoad|SocketFeed|FileFeed})", required = true)
    private IngestionType ingestType;

    public IngestionType getIngestType() {
        return ingestType;
    }

    @Option(name = "-d", aliases = "--datagen-duration", usage = "Data generation datagenDuration in seconds", metaVar = "DATAGENDURATION")
    private int datagenDuration;

    public int getDatagenDuration() {
        return datagenDuration;
    }

    @Option(name = "-dc", aliases = "--datagen-count", usage = "Number of records for data generation")
    private long datagenCount;

    public long getDatagenCount() {
        return datagenCount;
    }

    @Option(name = "-qd", aliases = "--querygen-duration", usage = "Query generation duration in seconds", metaVar = "QUERYGENDURATION")
    private int queryDuration;

    public int getQueryDuration() {
        return queryDuration;
    }

    @Option(name = "-qc", aliases = "--querygen-count", usage = "Number of generated queries", metaVar = "QUERYGENDURATION")
    private int queryCount;

    @Option(name = "-qo", aliases = "--querygen-output", usage = "The output file path for query generation")
    private String qgenOutputFilePath;

    public String getQgenOutputFilePath() {
        return qgenOutputFilePath;
    }

    public int getQueryCount() {
        return queryCount;
    }

    @Option(name = "-regex", aliases = "--regex", usage = "Regular expression used to match experiment names", metaVar = "REGEXP")
    private String regex;

    public String getRegex() {
        return regex;
    }

    @Option(name = "-rf", aliases = "--results-file", usage = "File containing results of experiments")
    private String resultsFile = null;

    public String getResultsFile() {
        return resultsFile;
    }

    @Option(name = "-sf", aliases = "--stat-file", usage = "Enable IO/CPU stats and place in specified file")
    private String statFile = null;

    public String getStatFile() {
        return statFile;
    }

    @Option(name = "-ni", aliases = "--num-intervals", usage = "Number of data intervals to use when generating data")
    private int numIntervals = 1;

    public int getNIntervals() {
        return numIntervals;
    }

    @Option(name = "-rcbi", aliases = "--record-count-per-batch-during-ingestion-only", usage = "Record count per batch during ingestion only")
    private int recordCountPerBatchDuringIngestionOnly = 1000;

    public int getRecordCountPerBatchDuringIngestionOnly() {
        return recordCountPerBatchDuringIngestionOnly;
    }

    @Option(name = "-rcbq", aliases = "--record-count-per-batch-during-query", usage = "Record count per batch during query")
    private int recordCountPerBatchDuringQuery = 1000;

    public int getRecordCountPerBatchDuringQuery() {
        return recordCountPerBatchDuringQuery;
    }

    @Option(name = "-dsti", aliases = "--data-gen-sleep-time-during-ingestion-only", usage = "DataGen sleep time in milliseconds after every recordCountPerBatchDuringIngestionOnly records were sent")
    private long dataGenSleepTimeDuringIngestionOnly = 1;

    public long getDataGenSleepTimeDuringIngestionOnly() {
        return dataGenSleepTimeDuringIngestionOnly;
    }

    @Option(name = "-dstq", aliases = "--data-gen-sleep-time-during-query", usage = "DataGen sleep time in milliseconds after every recordCountPerBatchDuringQuery records were sent")
    private long dataGenSleepTimeDuringQuery = 1;

    public long getDataGenSleepTimeDuringQuery() {
        return dataGenSleepTimeDuringQuery;
    }

    @Option(name = "-o", aliases = "--output-dir", usage = "Directory for experiment output")
    private String outputDir;

    public String getOutputDir() {
        return outputDir;
    }

    @Option(name = "-s", aliases = "--seed", usage = "The seed parameter for random generators")
    private long seed = -1;

    public long getSeed() {
        return seed;
    }

    @Option(name = "-cn", aliases = "--max-components-num", usage = "The maximum number of LSM components (for prefix-based policies)")
    private int componentsNum = 5;

    public int getComponentsNum() {
        return componentsNum;
    }
}

