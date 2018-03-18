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
package org.apache.asterix.experiment.client.numgen;

import java.util.Random;

import org.apache.asterix.experiment.client.RangeGenerator;
import org.apache.asterix.experiment.client.numgen.ZipfDecreasingSpreadGenerator.ZipfCuspMaxSpreadGenerator;
import org.apache.asterix.experiment.client.numgen.ZipfDecreasingSpreadGenerator.ZipfCuspMinSpreadGenerator;
import org.apache.asterix.experiment.client.numgen.ZipfDecreasingSpreadGenerator.ZipfIncreasingSpreadGenerator;
import org.apache.asterix.experiment.client.numgen.ZipfDecreasingSpreadGenerator.ZipfRandomSpreadGenerator;

public enum DistributionType {
    Uniform, ZipfRegular, ZipfIncreasing, ZipfCuspMin, ZipfCuspMax, ZipfRandom;

    public static int INITIAL_SPREAD_ZIPF_SERIES_SIZE = 1024;

    public static NumberGenerator getGenerator(DistributionType spreadDistribution, DistributionType freqDistribution,
            int spreadMaxValue, int spreadMinValue, double alpha, long seed, int partition, int partitionIncrement,
            int minSpreadLength) {
        long spreadTotalSizeBound = spreadMaxValue - spreadMinValue;
        PrecomputedDistributionGenerator spreadGenerator =
                getNumberGenerator(spreadDistribution, INITIAL_SPREAD_ZIPF_SERIES_SIZE, spreadTotalSizeBound,
                        spreadMinValue, minSpreadLength, alpha, seed);
        PrecomputedDistributionGenerator freqGenerator =
                getNumberGenerator(freqDistribution, spreadGenerator.getSize(), 1, 0, 0, alpha, seed);
        //correlate spreads produced by spreadGenerator with probability of generating each one according to freqGenerator
        return new CorrelatedGenerator(spreadGenerator, freqGenerator, partition, partitionIncrement);
    }

    public static PrecomputedDistributionGenerator getNumberGenerator(DistributionType spreadDistribution, int size,
            final long sumBound, final long lowerBound, int minValue, double alpha, long seed) {
        switch (spreadDistribution) {
            case Uniform:
                return new PrecomputedDistributionGenerator() {
                    private Random r = new Random(seed);

                    @Override
                    public long getDistributionPrefix(int pos) {
                        return pos * minValue - RangeGenerator.adjustSign(lowerBound);
                    }

                    @Override
                    public int getRandomPosition() {
                        return r.nextInt(size);
                    }

                    @Override
                    public int getSize() {
                        return (int) sumBound;
                    }

                };
            case ZipfRegular:
                return new ZipfDecreasingSpreadGenerator(size, sumBound, lowerBound, minValue, alpha, seed);
            case ZipfRandom:
                return new ZipfRandomSpreadGenerator(size, sumBound, lowerBound, minValue, alpha, seed);
            case ZipfIncreasing:
                return new ZipfIncreasingSpreadGenerator(size, sumBound, lowerBound, minValue, alpha, seed);
            case ZipfCuspMin:
                return new ZipfCuspMinSpreadGenerator(size, sumBound, lowerBound, minValue, alpha, seed);
            case ZipfCuspMax:
                return new ZipfCuspMaxSpreadGenerator(size, sumBound, lowerBound, minValue, alpha, seed);
            default:
                throw new IllegalArgumentException("Unknown distribution type " + spreadDistribution);
        }
    }
}