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

import org.apache.asterix.experiment.client.numgen.DistributionType;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class TweetGeneratorStatisticsConfig {

    @Argument(required = false, usage = "Distribution type for the number generators [] (default = Uniform)")
    private DistributionType distributionType = DistributionType.Uniform;

    public DistributionType getDistributionType() {
        return distributionType;
    }

    @Option(name = "-s", aliases = "--skew-exponent", usage = "Starting partition number for the set of data generators (default = 1.0)")
    private double skewExponent;

    public double getSkewExponent() {
        return skewExponent;
    }
}
