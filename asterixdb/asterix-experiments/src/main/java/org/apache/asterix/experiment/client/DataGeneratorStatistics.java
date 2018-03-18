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
import org.apache.asterix.experiment.client.numgen.NumberGenerator;
import org.apache.asterix.external.util.IDGenerator;

public class DataGeneratorStatistics extends DataGeneratorForSpatialIndexEvaluation {

    private NumberGenerator[] valueGenArray = new NumberGenerator[NUM_BTREE_EXTRA_FIELDS];

    public DataGeneratorStatistics(StatisticsInitializationInfo info) {
        super(info);
        DistributionType[] freqDistributions = new DistributionType[] { DistributionType.Uniform,
                DistributionType.ZipfRegular, DistributionType.ZipfRandom };
        DistributionType[] spreadDistributions = DistributionType.values();
        int c = 0;
        for (int j = 0; j < spreadDistributions.length; j++) {
            for (int i = 0; i < freqDistributions.length; i++) {
                valueGenArray[c++] = DistributionType.getGenerator(spreadDistributions[j], freqDistributions[i],
                        info.domainMax, info.domainMin, info.skewCoeff, info.seed, info.partition,
                        info.partitionIncrement, info.minSpread);
            }
        }
    }

    @Override
    public TweetMessageIterator getTweetIterator(int duration, IDGenerator generator) {
        return new TweetMessageStatisticsIterator(duration, generator);
    }

    public class TweetMessageStatisticsIterator extends TweetMessageIterator {

        public TweetMessageStatisticsIterator(int duration, IDGenerator idGen) {
            super(duration, idGen);
        }

        private int[] btreeExtraFieldArray = new int[NUM_BTREE_EXTRA_FIELDS];

        @Override
        public TweetMessageSpatialIndex next() {
            getTwitterUser(null);
            Message message = randMessageGen.getNextRandomMessage();
            Point location = randLocationGen.getRandomPoint();
            DateTime sendTime = randDateGen.getNextRandomDatetime();
            for (int i = 0; i < NUM_BTREE_EXTRA_FIELDS; i++) {
                btreeExtraFieldArray[i] = (int) Math.floor(valueGenArray[i].getValue());
            }
            twMessage.reset(idGen.getNextULong(), twUser, location, sendTime, message.getReferredTopics(), message,
                    btreeExtraFieldArray, DUMMY_SIZE_ADJUSTER);
            return twMessage;
        }

    }

    public static class StatisticsInitializationInfo extends SpatialIndexInitializationInfo {
        public StatisticsInitializationInfo(double skewCoeff, int domainMax, int domainMin, long seed, int partition,
                int partitionIncrement, int minSpread) {
            this.skewCoeff = skewCoeff;
            this.domainMax = domainMax;
            this.domainMin = domainMin;
            this.partition = partition;
            this.partitionIncrement = partitionIncrement;
            if (seed == -1) {
                this.seed = System.currentTimeMillis();
            } else {
                this.seed = seed;
            }
            this.minSpread = minSpread;
        }

        public final double skewCoeff;
        public final int domainMax;
        public final int domainMin;
        public final int partition;
        public final int partitionIncrement;
        public final long seed;
        public final int minSpread;
    }

}