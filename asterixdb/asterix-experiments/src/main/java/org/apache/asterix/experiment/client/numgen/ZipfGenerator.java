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
import java.util.Arrays;
import java.util.Random;

import org.apache.asterix.experiment.client.RangeGenerator;
import org.apache.commons.math3.util.FastMath;

public class ZipfGenerator implements NumberGenerator, PrecomputedDistributionGenerator {

    protected final long lowerBound;
    protected double[] values;
    private double[] normalizedPrefixSum;
    protected long[] roundedScaledPrefixSum;
    protected int size;

    private final double alpha;
    private final Random r;

    private static double eps = 0.001;

    public ZipfGenerator(int size, double alpha, long sumBound, long lowerBound, double xmin, long seed) {
        ArrayList<Double> populateValues = new ArrayList<>(size);
        ArrayList<Double> populatePrefixSum = new ArrayList<>(size);
        this.alpha = alpha;
        this.lowerBound = lowerBound;
        this.r = new Random(seed);
        populate(0, size, 0, populateValues, populatePrefixSum);
        //if we require a certain value for the normalized the minimum number form the series
        this.size = size;

        if (xmin > 0)
            this.size = fitBound(0, size, sumBound, xmin, true, true, populateValues, populatePrefixSum);
        this.values = new double[this.size];
        this.normalizedPrefixSum = new double[this.size];
        this.roundedScaledPrefixSum = new long[this.size];
        normalize(sumBound, lowerBound, populateValues, populatePrefixSum.get(this.size - 1));
    }

    private void populate(int start, int size, double totalSum, ArrayList<Double> populateValues,
            ArrayList<Double> populatePrefixSum) {
        assert (populateValues.size() == start && populateValues.size() < start + size);
        // 1st pass, computing "raw" values of values according to zipf-distribution
        for (int i = start + 1; i <= start + size; i++) {
            //compute Zipf values using Riemann zeta function
            double value = 1.0 / (FastMath.pow((double) i, alpha));
            populateValues.add(value);
            totalSum += value;
            populatePrefixSum.add(totalSum);
        }
    }

    private int fitBound(int start, int size, long sumBound, double xmin, boolean growPhase, boolean firstPass,
            ArrayList<Double> populateValues, ArrayList<Double> populatePrefixSum) {
        assert (populateValues.size() >= start + size);
        //normalized value of the smallest value
        double minValue = populateValues.get(start + size - 1) * sumBound / populatePrefixSum.get(start + size - 1);
        int newSize;
        if (growPhase)
            newSize = size * 2;
        else
            newSize = size / 2;
        // don't increase size on the first iteration
        if (firstPass)
            newSize = size;
        // stop fitting if the smallest value is close enough to the defined minimum
        if (Math.abs(minValue - xmin) < eps || size == 0)
            return start;
        else if (minValue > xmin) {
            //we need to generate more values in Riemann zeta series
            if (start + size + newSize > populateValues.size()) {
                int populateStart = Math.min(start + size, populateValues.size());
                populate(populateStart, newSize - (populateStart - start - size),
                        populatePrefixSum.get(start + size - 1), populateValues, populatePrefixSum);
            }
            return fitBound(start + size, newSize, sumBound, xmin, growPhase, false, populateValues, populatePrefixSum);
        } else {
            newSize = size / 2;
            //we generated more values than it's needed. Try fitting with smaller size
            return fitBound(start, newSize, sumBound, xmin, false, false, populateValues, populatePrefixSum);
        }
    }

    protected void normalize(long sumBound, long lowerBound, ArrayList<Double> populateValues, double sum) {
        double prefix = 0;
        for (int i = 0; i < size; i++) {
            values[i] = populateValues.get(i) / sum * sumBound;
            prefix += populateValues.get(i) / sum;
            normalizedPrefixSum[i] = Math.min(prefix, 1.0);
            roundedScaledPrefixSum[i] = Math.round(prefix * sumBound - RangeGenerator.adjustSign(lowerBound));
        }
    }

    private long round(double val) {
        long floor = (long) Math.floor(val);
        double rand = r.nextDouble();
        if (rand >= (val - floor))
            return floor;
        else
            return floor + 1;
    }

    public int getRandomPosition() {
        Double rand = r.nextDouble();
        int pos = Arrays.binarySearch(normalizedPrefixSum, rand);
        if (pos >= 0)
            // if the value was found in the prefix sum array return it right after
            return pos;
        else
            //else convert position using formula for Arrays.binarySearch() result: (-potential_insertion_point - 1)
            return -(pos + 1);
    }

    @Override
    public int getSize() {
        return size;
    }

    public long getDistributionPrefix(int position) {
        return roundedScaledPrefixSum[position];
    }

    public double getValue() {
        return values[getRandomPosition()];
    }
}
