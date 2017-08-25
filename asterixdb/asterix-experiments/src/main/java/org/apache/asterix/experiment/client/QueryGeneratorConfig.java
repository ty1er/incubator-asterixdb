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

import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig.WorkloadType;
import org.kohsuke.args4j.Option;

public class QueryGeneratorConfig {
    @Option(name = "-p", aliases = "--partition-num", usage = "Number of partitions for data generators (default = 1)")
    private int partitionNum = 1;
    @Option(name = "-qd", aliases = { "--query-duration" }, usage = "Duration in seconds to run guery generation")
    private int queryGenDuration = 0;
    @Option(name = "-qc", aliases = { "--query-count" }, usage = "The number of queries to generate")
    private int queryCount = -1;
    @Option(name = "-rh", aliases = "--rest-host", usage = "Asterix REST API host address", required = true, metaVar = "HOST")
    private String restHost;
    @Option(name = "-rp", aliases = "--rest-port", usage = "Asterix REST API port", required = true, metaVar = "PORT")
    private int restPort;

    @Option(name = "-o", aliases = "--output-file", usage = "A file to save generated query execution results", required = true)
    private String output;

    @Option(name = "-wt", aliases = "--workload-type", usage = "The type of experiment workload", required = true)
    private WorkloadType workloadType;

    @Option(name = "-s", aliases = "--seed", usage = "The seed parameter for random generators")
    private long seed = System.currentTimeMillis();

    public long getSeed() {
        return seed;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public int getDuration() {
        return queryGenDuration;
    }

    public int getQueryCount() {
        return queryCount;
    }

    public String getRESTHost() {
        return restHost;
    }

    public int getRESTPort() {
        return restPort;
    }

    public String getOutput() {
        return output;
    }

    public WorkloadType getWorkloadType() {
        return workloadType;
    }
}
