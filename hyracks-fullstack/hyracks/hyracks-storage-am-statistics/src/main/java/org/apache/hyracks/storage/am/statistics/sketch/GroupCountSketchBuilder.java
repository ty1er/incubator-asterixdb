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
package org.apache.hyracks.storage.am.statistics.sketch;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletSynopsis;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class GroupCountSketchBuilder extends AbstractSynopsisBuilder<WaveletSynopsis> {
    private final static Logger LOGGER = Logger.getLogger(GroupCountSketch.class.getName());
    private final int fanout;
    private final int fanoutLog;
    private final int levelNum;
    private final double energyThreshold;

    private GroupCountSketch[] sketches;

    public GroupCountSketchBuilder(WaveletSynopsis synopsis, String dataverse, String dataset, String index,
            String field, boolean isAntimatter, IFieldExtractor fieldExtractor, ComponentStatistics componentStatistics,
            int fanout, double failureProbability, double accuracy, double energyAccuracy, long inputSize, int seed) {
        super(synopsis, dataverse, dataset, index, field, isAntimatter, fieldExtractor, componentStatistics);
        this.fanout = fanout;
        fanoutLog = (int) Math.round(Math.log(fanout) / Math.log(2.0));
        levelNum = synopsis.getMaxLevel() / fanoutLog;

        // number of independent testsNum used to randomize sketch is determined as O(log(1/δ))
        int testsNum = (int) Math.round(Math.log(1.0 / failureProbability));
        // number of buckets is determined as O(1/ε)
        int bucketNum = (int) Math.round(1.0 / accuracy);
        // number of subbuckets is determined as O(1/(ε^2))
        int subbucketNum = bucketNum * bucketNum;
        // threshold 𝜙 is determined as stream size multiplied by (𝜂*ε)/B
        if (inputSize > 0) {
            energyThreshold = inputSize * (accuracy * energyAccuracy) / synopsis.getSize();
        } else {
            energyThreshold = Double.MAX_VALUE;
        }

        Random random = new Random(seed);
        // Use MurmurHash to generate pairwise independent hash functions. Refer to "Less hashing, same performance:
        // Building a better bloom filter" ESA'06 for explanations
        HashFunction murmurHash = Hashing.murmur3_128(seed);

        sketches = new GroupCountSketch[levelNum];
        int totalSketchSize = 0;
        for (int i = 0; i < levelNum; i++) {
            //        gcSketch = new GroupCountSketch(this.levelNum + 1, depth, width, fanoutLog);
            sketches[i] = new GroupCountSketch(testsNum, bucketNum, subbucketNum, random, murmurHash);
            totalSketchSize += sketches[i].getSketchSize();
        }
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Allocating " + totalSketchSize + " bytes to GroupCount sketch ");
        }
    }

    @Override
    public void addValue(long position) {
        double value = 1.0;
        WaveletCoefficient coeff = new WaveletCoefficient(value, 0, position);
        //translate adding coeff on level 0 to updates in upper levels of error tree
        while (coeff.getKey() != 0 || coeff.getLevel() == 0) {
            long parentIdx = coeff.getParentCoeffIndex(synopsis.getDomainStart(), synopsis.getMaxLevel());
            double parentValue = value;
            int parentLevel;
            //main average coefficient
            if (coeff.getLevel() == synopsis.getMaxLevel()) {
                parentLevel = coeff.getLevel();
            } else {
                int sign = ((coeff.getKey() & 0x1) == 1) ? -1 : 1;
                parentValue *= sign;
                parentLevel = coeff.getLevel() + 1;
            }
            parentValue /= (1 << parentLevel);
            if (synopsis.isNormalized()) {
                parentValue /=
                        WaveletCoefficient.getNormalizationCoefficient(synopsis.getMaxLevel(), parentLevel * fanoutLog);
            }
            coeff.reset(parentValue, parentLevel, parentIdx);
            updateSketchBinaryTree(coeff);
        }

        //        item += 1 << (levelNum * fanoutLog);
        //transform update into wavelet domain
        //        long div = 1;
        //        for (int i = 0; i < levelNum; i++) {
        //            //            Long coeffIdx = (long) ((1 << ((levelNum - i) * fanoutLog)) + item);
        //            item >>= (fanoutLog - 1);
        //            int sign = (item & 1) == 0 ? 1 : -1;
        //            item >>= 1;
        //            double normCoeff = WaveletCoefficient.getNormalizationCoefficient(levelNum * fanoutLog,
        //                    (i + 1) * fanoutLog);
        //            div = (1 << ((i + 1) * fanoutLog));
        //
        //            gcSketch.update(item, diff * sign / (normCoeff * div));
        //        }
        //        gcSketch.update(0, diff / div);

        //for (j = 0, group = coeffIdx; j < levels; j += fanout, group >>= fanout) {
    }

    private void updateSketchBinaryTree(WaveletCoefficient coeff) {
        long groupIdx = coeff.getKey();
        for (int i = 0; i < levelNum; i++, groupIdx >>= fanoutLog) {
            sketches[i].update(coeff.getKey(), groupIdx, coeff.getValue());
        }
    }

    @Override
    public void finishSynopsisBuild() throws HyracksDataException {
        //recursively traverse sketches of various levels to get the highest-energy coefficients
        for (int i = 0; i < fanout; i++) {
            findHighestEnergyCoeffs(levelNum - 1, i);
        }
        synopsis.orderByKeys();
    }

    private void findHighestEnergyCoeffs(int level, long groupIdx) {
        double energyEstimate = sketches[level].estimateL2(groupIdx);
        if (energyEstimate >= energyThreshold) {
            if (level == 0) {
                double coeffValue = sketches[level].estimateValue(groupIdx);
                // coeff value was already normalized when it was added to the sketch
                boolean isNormalized = false;
                synopsis.addElement(groupIdx, coeffValue, WaveletCoefficient.getLevel(groupIdx, synopsis.getMaxLevel()),
                        isNormalized);
            } else {
                for (int i = 0; i < fanout; i++) {
                    long childGroupIdx = (groupIdx << fanoutLog) + i;
                    findHighestEnergyCoeffs(level - 1, childGroupIdx);
                }
            }
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        //Noop
    }
}
