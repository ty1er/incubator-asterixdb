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

import static org.apache.asterix.experiment.client.numgen.DistributionType.INITIAL_SPREAD_ZIPF_SERIES_SIZE;

import java.util.Random;

import org.apache.asterix.experiment.client.numgen.DistributionType;
import org.apache.asterix.experiment.client.numgen.PrecomputedDistributionGenerator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.RandomGeneratorFactory;

public abstract class RangeGenerator {

    public static int MIN_SPREAD = 1;

    protected RandomDataGenerator randGen;
    protected PrecomputedDistributionGenerator dataGen;
    protected long upperBound;
    protected long lowerBound;

    public abstract Pair<Long, Long> getNextRange();

    public RangeGenerator(DistributionType spreadDistribution, long upperBound, long lowerBound, double skew,
            long seed) {
        this.randGen = new RandomDataGenerator(RandomGeneratorFactory.createRandomGenerator(new Random(seed)));
        this.dataGen = DistributionType
                .getNumberGenerator(spreadDistribution, INITIAL_SPREAD_ZIPF_SERIES_SIZE, upperBound - lowerBound,
                        lowerBound, MIN_SPREAD, skew, seed);
        this.upperBound = upperBound;
        this.lowerBound = lowerBound;
    }

    public static long adjustSign(long value) {
        return value * Long.signum(value);
    }

    public static RangeGenerator getRangeGenerator(StatisticsRangeType queryType, long upperBound, long lowerBound,
            int length, double percentage, int queryCount, DistributionType spreadDistribution, double dataSkew,
            long seed) {
        switch (queryType) {
            case Random:
                return new RandomRangeGenerator(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
            case CorrelatedRandom:
                return new CorrelatedRandomRangeGenerator(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
            case FixedLength:
                return new FixedLengthRangeGenerator(length, spreadDistribution, upperBound, lowerBound, dataSkew,
                        seed);
            case HalfOpen:
                return new HalfOpenRangeGenerator(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
            //            case CorrelatedHalfOpen:
            //                return new HalfOpenCorrelatedRangeGenerator(upperBound, spreadDistribution, dataCount, dataSkew);
            case Point:
                return new PointGenerator(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
            case CorrelatedPoint:
                return new CorrelatedPointGenerator(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
            case CDF:
                return new CDFGenerator(spreadDistribution, upperBound, lowerBound, queryCount, dataSkew, seed);
            case Percentage:
                return new PercentageRangeGenerator(percentage, spreadDistribution, upperBound, lowerBound, dataSkew,
                        seed);
            default:
                throw new IllegalArgumentException("Cannot create query generator of the type " + queryType);
        }
    }

    static class RandomRangeGenerator extends RangeGenerator {

        public RandomRangeGenerator(DistributionType spreadDistribution, long upperBound, long lowerBound,
                double dataSkew, long seed) {
            super(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
        }

        @Override
        public Pair<Long, Long> getNextRange() {
            long start = randGen.nextLong(lowerBound, upperBound);
            long end = randGen.nextLong(start, upperBound);
            return Pair.of(start, end);
        }

    }

    static class CorrelatedRandomRangeGenerator extends RangeGenerator {

        public CorrelatedRandomRangeGenerator(DistributionType spreadDistribution, long upperBound, long lowerBound,
                double dataSkew, long seed) {
            super(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
        }

        @Override public Pair<Long, Long> getNextRange() {
            int correlatedStartDataPoint = randGen.nextInt(0, dataGen.getSize());
            int correlatedEndDataPoint = randGen.nextInt(correlatedStartDataPoint, dataGen.getSize());
            long start = dataGen.getDistributionPrefix(correlatedStartDataPoint);
            long end = dataGen.getDistributionPrefix(correlatedEndDataPoint);
            return Pair.of(start, end);
        }
    }

    static class FixedLengthRangeGenerator extends RangeGenerator {

        private long length;

        public FixedLengthRangeGenerator(long length, DistributionType spreadDistribution, long upperBound,
                long lowerBound, double dataSkew, long seed) {
            super(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
            this.length = length;
        }

        @Override
        public Pair<Long, Long> getNextRange() {
            long bound = upperBound - length;
            if (bound <= lowerBound) {
                bound = upperBound;
            }
            long start;
            if (lowerBound == upperBound) {
                start = lowerBound;
            } else {
                start = randGen.nextLong(lowerBound, bound);
            }
            return Pair.of(start, Math.min(upperBound, start + length));
        }

    }

    static class HalfOpenRangeGenerator extends RangeGenerator {

        public HalfOpenRangeGenerator(DistributionType spreadDistribution, long upperBound, long lowerBound,
                double dataSkew, long seed) {
            super(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
        }

        @Override
        public Pair<Long, Long> getNextRange() {
            long start = randGen.nextLong(lowerBound, upperBound);
            return Pair.of(start, (long) upperBound);
        }

    }

    static class PointGenerator extends RangeGenerator {

        public PointGenerator(DistributionType spreadDistribution, long upperBound, long lowerBound, double dataSkew,
                long seed) {
            super(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
        }

        @Override
        public Pair<Long, Long> getNextRange() {
            long randDataPoint = randGen.nextLong(lowerBound, upperBound);
            return Pair.of(randDataPoint, randDataPoint);
        }

    }

    static class CorrelatedPointGenerator extends RangeGenerator {

        public CorrelatedPointGenerator(DistributionType spreadDistribution, long upperBound, long lowerBound,
                double dataSkew, long seed) {
            super(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
        }

        @Override
        public Pair<Long, Long> getNextRange() {
            int correlatedDataPoint = randGen.nextInt(0, dataGen.getSize());
            long pointVal = dataGen.getDistributionPrefix(correlatedDataPoint);
            return Pair.of(pointVal, pointVal);
        }

    }

    static class FrequentPointGenerator extends RangeGenerator {

        public FrequentPointGenerator(DistributionType spreadDistribution, long upperBound, long lowerBound,
                double dataSkew, long seed) {
            super(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
        }

        @Override
        public Pair<Long, Long> getNextRange() {
            int randDataPoint = dataGen.getRandomPosition();
            long pointVal = dataGen.getDistributionPrefix(randDataPoint);
            return Pair.of(pointVal, pointVal);
        }

    }

    static class CDFGenerator extends RangeGenerator {
        private final int queryCount;
        private int i;

        public CDFGenerator(DistributionType spreadDistribution, long upperBound, long lowerBound, int queryCount,
                double dataSkew, long seed) {
            super(spreadDistribution, upperBound, lowerBound, dataSkew, seed);
            this.queryCount = queryCount;
            this.i = 0;
        }

        @Override public Pair<Long, Long> getNextRange() {
            long totalLength = upperBound - lowerBound;
            return Pair.of((long) lowerBound,
                    (long) (totalLength / queryCount) * i++ - RangeGenerator.adjustSign(lowerBound));

        }
    }

    static class PercentageRangeGenerator extends FixedLengthRangeGenerator {
        public PercentageRangeGenerator(double percentage, DistributionType spreadDistribution, long upperBound,
                long lowerBound, double dataSkew, long seed) {
            super(((upperBound - lowerBound) * percentage) < MIN_SPREAD ? MIN_SPREAD
                    : (long) Math.floor((upperBound - lowerBound) * percentage), spreadDistribution, upperBound,
                    lowerBound, dataSkew, seed);
        }
    }
}
