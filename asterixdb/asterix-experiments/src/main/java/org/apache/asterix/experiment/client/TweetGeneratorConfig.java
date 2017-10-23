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

public class TweetGeneratorConfig {

    @Option(name = "-wt", aliases = { "--workload-type" }, usage = "Whether to use change feed for data generation")
    private LSMExperimentSetRunnerConfig.WorkloadType workloadType;

    public LSMExperimentSetRunnerConfig.WorkloadType getWorkloadType() {
        return workloadType;
    }

    @Option(name = "-pid", aliases = "--partition-id", usage = "The partition id in order to avoid key duplication", required = true)
    private int partitionId;

    public int getPartitionId() {
        return partitionId;
    }

    @Option(name = "-pn", aliases = "--partition-num", usage = "Total number of data generator partitions(default = 1)", required = true)
    private int partitionsNum = 1;

    public int getPartitionsNum() {
        return partitionsNum;
    }

    @Option(name = "-s", aliases = "--seed", usage = "The seed parameter for random generators")
    private long seed = System.currentTimeMillis();

    public long getSeed() {
        return seed;
    }

    @Option(name = "-oh", aliases = "--orchestrator-host", usage = "The host name of the orchestrator")
    private String orchHost;

    public String getOrchestratorHost() {
        return orchHost;
    }

    @Option(name = "-op", aliases = "--orchestrator-port", usage = "The port number of the orchestrator")
    private int orchPort;

    public int getOrchestratorPort() {
        return orchPort;
    }

    @Option(name = "-dc", aliases = "--datagen-count", usage = "The number of record count for datagen")
    private int datagenCount;

    public int getDatagenCount() {
        return datagenCount;
    }

    @Option(name = "-ub", aliases = "--datagen-upper-bound", usage = "Upper bound of generated data values")
    private int upperBound = Byte.MAX_VALUE;

    public int getUpperBound() {
        return upperBound;
    }

    @Option(name = "-lb", aliases = "--datagen-lower-bound", usage = "Lower bound of generated data values")
    private int lowerBound = Byte.MIN_VALUE;

    public int getLowerBound() {
        return lowerBound;
    }

    @Option(name = "-pu", aliases = "--percentage-updates", usage = "The percentage of updated tweets in the stream")
    private double updatesPercent;

    public double getUpdatesPercentage() {
        return updatesPercent;
    }

    @Option(name = "-pd", aliases = "--percentage-deletes", usage = "The percentage of deleted tweets in the stream")
    private double deletesPercent;

    public double getDeletesPercentage() {
        return deletesPercent;
    }

    @Option(name = "-z", aliases = "--zipf-skew", usage = "The skew parameter for Zipifan distribution")
    private Double skew;

    public Double getSkew() {
        return skew;
    }
}
