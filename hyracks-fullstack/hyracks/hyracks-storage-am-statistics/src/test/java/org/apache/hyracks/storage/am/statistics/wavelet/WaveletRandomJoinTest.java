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
package org.apache.hyracks.storage.am.statistics.wavelet;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformHelper;
import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformTuple;
import org.apache.hyracks.test.support.RepeatRule;
import org.apache.hyracks.test.support.RepeatRule.Repeat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class WaveletRandomJoinTest extends WaveletTest {

    private WaveletSynopsis leftSynopsis;
    private WaveletSynopsis rightSynopsis;
    private WaveletSynopsis joinedSynopsis;
    private WaveletSynopsis expectedSynopsis;
    private RandomDataGenerator rnd;

    private static int NUM_RECORDS = 50;

    public WaveletRandomJoinTest(int maxLevel, long domainStart, long domainEnd) {
        super(Integer.MAX_VALUE, maxLevel, true, domainStart, domainEnd);
        rnd = new RandomDataGenerator();
    }

    private class NonEmptyWaveletTransform extends WaveletTransform {

        public NonEmptyWaveletTransform(WaveletSynopsis synopsis, boolean isAntimatter, IFieldExtractor fieldExtractor,
                ComponentStatistics componentStatistics) {
            super(synopsis, isAntimatter, fieldExtractor, componentStatistics);
        }

        @Override
        public void addValue(long tuplePosition) {
            super.addValue(tuplePosition);
            isEmpty = false;
        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { Byte.SIZE, Byte.MIN_VALUE, Byte.MAX_VALUE },
                { Short.SIZE, Short.MIN_VALUE, Short.MAX_VALUE },
                { Integer.SIZE, Integer.MIN_VALUE, Integer.MAX_VALUE },
                { Long.SIZE, Long.MIN_VALUE, Long.MAX_VALUE } });
    }

    private List<TransformTuple> generateRandomData(int numRecords) {
        List<TransformTuple> result = new ArrayList<>();
        Set<Long> generatedPositions = new HashSet<>();
        for (int i = 0; i < numRecords; i++) {
            Long newPosition;
            do {
                newPosition = rnd.nextLong(domainStart, domainEnd);
            } while (generatedPositions.contains(newPosition));
            generatedPositions.add(newPosition);
            result.add(new TransformTuple(newPosition, rnd.nextLong(1, 10)));
        }
        return result;
    }

    private List<TransformTuple> generateCorrelatedData(List<TransformTuple> inputData) {
        List<TransformTuple> result = generateRandomData(NUM_RECORDS / 2);
        Set<Long> generatedPositions = result.stream().map(x -> x.position).collect(Collectors.toSet());
        while (result.size() < NUM_RECORDS) {
            TransformTuple t = inputData.get(RandomUtils.nextInt(0, inputData.size()));
            if (!generatedPositions.contains(t.position) && RandomUtils.nextBoolean()) {
                result.add(new TransformTuple(t.position, rnd.nextLong(1, 10)));
                generatedPositions.add(t.position);
            }
        }
        return result;
    }

    private List<TransformTuple> getJoinedData(List<TransformTuple> rData, List<TransformTuple> sData) {
        List<TransformTuple> joinedResult = new ArrayList<>();
        Map<Long, Long> rCardinality = new HashMap<>();
        Map<Long, Long> sCardinality = new HashMap<>();
        for (TransformTuple t : rData) {
            rCardinality.put(t.position, t.cardinality);
        }
        for (TransformTuple t : sData) {
            sCardinality.put(t.position, t.cardinality);
        }
        for (Map.Entry<Long, Long> rItem : rCardinality.entrySet()) {
            Long sItemCardinality = sCardinality.get(rItem.getKey());
            if (sItemCardinality != null) {
                joinedResult.add(new TransformTuple(rItem.getKey(), rItem.getValue() * sItemCardinality));
            }
        }
        return joinedResult;
    }

    private List<TransformTuple> leftData;
    private List<TransformTuple> rightData;
    private List<TransformTuple> joinedData;

    @Before
    public void init() throws Exception {
        leftSynopsis = new WaveletSynopsis(domainStart, domainEnd, maxLevel, threshold,
                new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR), normalize, false);
        rightSynopsis = new WaveletSynopsis(domainStart, domainEnd, maxLevel, threshold,
                new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR), normalize, false);
        joinedSynopsis = new WaveletSynopsis(domainStart, domainEnd, maxLevel, threshold,
                new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR), normalize, false);
        expectedSynopsis = new WaveletSynopsis(domainStart, domainEnd, maxLevel, threshold,
                new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR), normalize, false);
        AbstractSynopsisBuilder leftBuilder = new NonEmptyWaveletTransform(leftSynopsis, false, null, null);
        AbstractSynopsisBuilder rightBuilder = new NonEmptyWaveletTransform(rightSynopsis, false, null, null);
        AbstractSynopsisBuilder joinedBuilder = new NonEmptyWaveletTransform(expectedSynopsis, false, null, null);
        leftData = generateRandomData(NUM_RECORDS);
        rightData = generateCorrelatedData(leftData);
        joinedData = getJoinedData(leftData, rightData);
        TransformHelper.runTransform(leftData, leftBuilder);
        TransformHelper.runTransform(rightData, rightBuilder);
        TransformHelper.runTransform(joinedData, joinedBuilder);
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Test
    @Repeat(times = 100)
    public void testJoinRelations() throws HyracksDataException {
        joinedSynopsis.join(leftSynopsis, rightSynopsis);
        joinedSynopsis.createBinaryPreorder();
        PeekingIterator<WaveletCoefficient> joinedIt = new PeekingIterator<>(joinedSynopsis.getElements().iterator());
        Iterator<WaveletCoefficient> compareIt = expectedSynopsis.getElements().iterator();
        while (compareIt.hasNext()) {
            WaveletCoefficient compareCoeff = compareIt.next();
            double joinedCoeffValue =
                    joinedSynopsis.findCoeffValue(joinedIt, compareCoeff.getKey(), compareCoeff.getLevel());
            assertEquals(compareCoeff.getValue(), joinedCoeffValue,
                    compareCoeff.getValue() == 0.0 ? epsilon : Math.abs(compareCoeff.getValue() * epsilon));
        }
    }
}
