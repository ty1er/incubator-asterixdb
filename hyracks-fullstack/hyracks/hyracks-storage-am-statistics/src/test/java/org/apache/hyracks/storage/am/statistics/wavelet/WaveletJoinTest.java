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

import java.util.Arrays;
import java.util.Collection;
import java.util.PriorityQueue;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class WaveletJoinTest extends WaveletTest {

    protected WaveletSynopsis leftSynopsis;
    protected WaveletSynopsis rightSynopsis;
    protected WaveletSynopsis joinedSynopsis;
    protected Collection<WaveletCoefficient> leftCoefficients;
    protected Collection<WaveletCoefficient> rightCoefficients;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { 4, 0, 15 }, { 4, -8, 7 }, { 4, -15, 0 } });
    }

    public WaveletJoinTest(int maxLevel, long domainStart, long domainEnd) {
        super(10, maxLevel, false, domainStart, domainEnd);
        leftCoefficients = new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR);
        rightCoefficients = new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR);
    }

    @Before
    public void init() throws Exception {
        leftSynopsis =
                new WaveletSynopsis(domainStart, domainEnd, maxLevel, threshold, leftCoefficients, normalize, false);
        rightSynopsis =
                new WaveletSynopsis(domainStart, domainEnd, maxLevel, threshold, rightCoefficients, normalize, false);
        joinedSynopsis = new WaveletSynopsis(domainStart, domainEnd, maxLevel, threshold,
                new PriorityQueue<>(WaveletCoefficient.VALUE_COMPARATOR), normalize, false);
    }

    @Test
    // [1,2,5] ⋈ [2,2,4] = [2,2]
    public void testJoinCoefficients() {
        // init coefficients for transformed relation [2,2,4]
        leftCoefficients.add(new WaveletCoefficient(0.1875, 5, 0));
        leftCoefficients.add(new WaveletCoefficient(-0.1875, 4, 1));
        leftCoefficients.add(new WaveletCoefficient(0.125, 3, 3));
        leftCoefficients.add(new WaveletCoefficient(-0.5, 2, 6));
        leftCoefficients.add(new WaveletCoefficient(0.25, 2, 7));
        leftCoefficients.add(new WaveletCoefficient(1, 1, 13));
        leftCoefficients.add(new WaveletCoefficient(0.5, 1, 14));
        // init coefficients for transformed relation [1,2,5]
        rightCoefficients.add(new WaveletCoefficient(0.1875, 5, 0));
        rightCoefficients.add(new WaveletCoefficient(-0.1875, 4, 1));
        rightCoefficients.add(new WaveletCoefficient(0.125, 3, 3));
        rightCoefficients.add(new WaveletCoefficient(0.25, 2, 7));
        rightCoefficients.add(new WaveletCoefficient(-0.5, 1, 12));
        rightCoefficients.add(new WaveletCoefficient(0.5, 1, 13));
        rightCoefficients.add(new WaveletCoefficient(-0.5, 1, 14));

        joinedSynopsis.join(leftSynopsis, rightSynopsis);
        joinedSynopsis.orderByKeys();
        PeekingIterator<WaveletCoefficient> it = new PeekingIterator<>(joinedSynopsis.getElements().iterator());

        assertEquals(0.125, joinedSynopsis.findCoeffValue(it, 0, 5), epsilon);
        assertEquals(-0.125, joinedSynopsis.findCoeffValue(it, 1, 4), epsilon);
        assertEquals(0.25, joinedSynopsis.findCoeffValue(it, 3, 3), epsilon);
        assertEquals(-0.5, joinedSynopsis.findCoeffValue(it, 6, 2), epsilon);
        assertEquals(0, joinedSynopsis.findCoeffValue(it, 7, 2), epsilon);
        assertEquals(0, joinedSynopsis.findCoeffValue(it, 12, 1), epsilon);
        assertEquals(1, joinedSynopsis.findCoeffValue(it, 13, 1), epsilon);
        assertEquals(0, joinedSynopsis.findCoeffValue(it, 14, 1), epsilon);
    }
}
