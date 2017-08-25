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

import java.util.ArrayList;
import java.util.Random;

public class ZipfDecreasingSpreadGenerator extends ZipfGenerator {

    public ZipfDecreasingSpreadGenerator(int size, long sumBound, long lowerBound, int minValue, double alpha,
            long seed) {
        super(size, alpha, sumBound, lowerBound, minValue, seed);
    }

    public static class ZipfIncreasingSpreadGenerator extends ZipfDecreasingSpreadGenerator {

        public ZipfIncreasingSpreadGenerator(int size, long sumBound, long lowerBound, int minValue, double alpha,
                long seed) {
            super(size, sumBound, lowerBound, minValue, alpha, seed);
        }

        @Override
        public long getDistributionPrefix(int position) {
            return roundedScaledPrefixSum[size - 1] - (roundedScaledPrefixSum[position] - lowerBound);
        }

    }

    public static class ZipfCuspMinSpreadGenerator extends ZipfDecreasingSpreadGenerator {

        public ZipfCuspMinSpreadGenerator(int size, long sumBound, long lowerBound, int minValue, double alpha,
                long seed) {
            super(size, sumBound / 2, lowerBound, minValue, alpha, seed);
        }

        @Override
        public long getDistributionPrefix(int position) {
            if (position < size)
                return roundedScaledPrefixSum[position];
            else
                return roundedScaledPrefixSum[size - 1] + (roundedScaledPrefixSum[size - 1] - roundedScaledPrefixSum[
                        position - size]);
        }

        @Override
        public int getSize() {
            return size * 2;
        }
    }

    public static class ZipfCuspMaxSpreadGenerator extends ZipfCuspMinSpreadGenerator {

        public ZipfCuspMaxSpreadGenerator(int size, long sumBound, long lowerBound, int minValue, double alpha,
                long seed) {
            super(size, sumBound, lowerBound, minValue, alpha, seed);
        }

        @Override
        public long getDistributionPrefix(int position) {
            if (position < size)
                return roundedScaledPrefixSum[size - 1] - (roundedScaledPrefixSum[size - position - 1] - lowerBound);
            else
                return roundedScaledPrefixSum[size - 1] + (roundedScaledPrefixSum[position - size] - lowerBound);
        }
    }

    public static class ZipfRandomSpreadGenerator extends ZipfGenerator {
        public ZipfRandomSpreadGenerator(int size, long sumBound, long lowerBound, int minValue, double alpha,
                long seed) {
            super(size, alpha, sumBound, lowerBound, minValue, seed);
            permute(seed, sumBound, lowerBound);
        }

        //Fisher-Yates permutation algorithm
        private void permute(long seed, long sumBound, long lowerBound) {
            int[] permutation = new int[size];
            Random r = new Random(seed);
            for (int i = 0; i < permutation.length; i++) {
                permutation[i] = i;
            }
            for (int i = permutation.length - 1; i >= 0; i--) {
                int j = r.nextInt(i + 1);
                //swap elements
                int tmp = permutation[i];
                permutation[i] = permutation[j];
                permutation[j] = tmp;
            }

            //reshuffle values according to produced permutation
            ArrayList<Double> shuffledValues = new ArrayList<>(values.length);
            double sum = 0;
            for (int i = 0; i < size; i++) {
                sum += values[permutation[i]];
                shuffledValues.add(values[permutation[i]]);
            }
            //renormalize the values, recomputing prefixSums
            normalize(sumBound, lowerBound, shuffledValues, sum);
        }
    }
}
