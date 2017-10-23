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

public class LSMStatsExperimentSetRunnerConfig extends LSMExperimentSetRunnerConfig {
    public LSMStatsExperimentSetRunnerConfig(String logDirSuffix) {
        super(logDirSuffix);
    }

    @Option(name = "-z", aliases = "--zipf-skew", usage = "The skew parameter for Zipifan distribution")
    private String skew;

    public String getSkew() {
        return skew;
    }

    @Option(name = "-rt", aliases = "--range-type", usage = "Range generator type [] (default = Random)")
    private StatisticsRangeType rangeType = StatisticsRangeType.Random;

    public StatisticsRangeType getRangeType() {
        return rangeType;
    }

    @Option(name = "-rl", aliases = "--range-length", usage = "The range for fix-sized range queries")
    private int rangeLength;

    public int getRangeLength() {
        return rangeLength;
    }

    @Option(name = "-pr", aliases = "--range-percent", usage = "The percentage of total domain used for range queries")
    private double rangePercent;

    public double getRangePercent() {
        return rangePercent;
    }

    @Option(name = "-ub", aliases = "--datagen-upper-bound", usage = "Upper bound of generated data values")
    private String upperBound;

    public String getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(String s) {
        upperBound = s;
    }

    @Option(name = "-lb", aliases = "--datagen-lower-bound", usage = "Lower bound of generated data values")
    private String lowerBound;

    public String getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(String s) {
        lowerBound = s;
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

    @Option(name = "-ss", aliases = "--synopsis-size", usage = "The size of synopsis (number of coefficients or buckets")
    private int synopsisSize = 100;

    public int getSynopsisSize() {
        return synopsisSize;
    }
}
