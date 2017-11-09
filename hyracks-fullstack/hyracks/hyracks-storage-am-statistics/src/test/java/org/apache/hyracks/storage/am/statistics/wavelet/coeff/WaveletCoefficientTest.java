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
package org.apache.hyracks.storage.am.statistics.wavelet.coeff;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hyracks.storage.am.statistics.wavelet.DyadicTupleRange;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class WaveletCoefficientTest {

    private static int MAX_LEVEL = 3;
    private final long domainStart;
    private final long domainEnd;
    private final long DOMAIN_LENGTH = 8;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { 0, 7 }, { -4, 3 }, { -8, 0 } });
    }

    public WaveletCoefficientTest(long domainStart, long domainEnd) {
        this.domainStart = domainStart;
        this.domainEnd = domainEnd;
    }

    @Test
    public void testParentCoeffLevel0() {
        WaveletCoefficient coeff0 = new WaveletCoefficient(0.0, 0, domainStart);
        WaveletCoefficient coeff1 = new WaveletCoefficient(0.0, 0, domainStart + 1);
        WaveletCoefficient coeff2 = new WaveletCoefficient(0.0, 0, domainStart + 2);
        WaveletCoefficient coeff3 = new WaveletCoefficient(0.0, 0, domainStart + 3);
        WaveletCoefficient coeff4 = new WaveletCoefficient(0.0, 0, domainStart + 4);
        WaveletCoefficient coeff5 = new WaveletCoefficient(0.0, 0, domainStart + 5);
        WaveletCoefficient coeff6 = new WaveletCoefficient(0.0, 0, domainStart + 6);
        WaveletCoefficient coeff7 = new WaveletCoefficient(0.0, 0, domainStart + 7);

        assertEquals(4, coeff0.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(4, coeff1.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(5, coeff2.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(5, coeff3.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(6, coeff4.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(6, coeff5.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(7, coeff6.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(7, coeff7.getParentCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testParentCoeffLevel1() {
        WaveletCoefficient coeff4 = new WaveletCoefficient(0.0, 1, 4);
        WaveletCoefficient coeff5 = new WaveletCoefficient(0.0, 1, 5);
        WaveletCoefficient coeff6 = new WaveletCoefficient(0.0, 1, 6);
        WaveletCoefficient coeff7 = new WaveletCoefficient(0.0, 1, 7);

        assertEquals(2, coeff4.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(2, coeff5.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(3, coeff6.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(3, coeff7.getParentCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testParentCoeffLevel2() {
        WaveletCoefficient coeff2 = new WaveletCoefficient(0.0, 2, 2);
        WaveletCoefficient coeff3 = new WaveletCoefficient(0.0, 2, 3);

        assertEquals(1, coeff2.getParentCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(1, coeff3.getParentCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testParentCoeffLevel3() {
        WaveletCoefficient coeff1 = new WaveletCoefficient(0.0, 3, 1);

        assertEquals(0, coeff1.getParentCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testLeftChildCoeffLevel1() {
        WaveletCoefficient coeff4 = new WaveletCoefficient(0.0, 1, 4);
        WaveletCoefficient coeff5 = new WaveletCoefficient(0.0, 1, 5);
        WaveletCoefficient coeff6 = new WaveletCoefficient(0.0, 1, 6);
        WaveletCoefficient coeff7 = new WaveletCoefficient(0.0, 1, 7);

        assertEquals(domainStart, coeff4.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 2, coeff5.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 4, coeff6.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 6, coeff7.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testLeftChildCoeffLevel2() {
        WaveletCoefficient coeff2 = new WaveletCoefficient(0.0, 2, 2);
        WaveletCoefficient coeff3 = new WaveletCoefficient(0.0, 2, 3);

        assertEquals(4, coeff2.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(6, coeff3.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testLeftChildCoeffLevel3() {
        WaveletCoefficient coeff1 = new WaveletCoefficient(0.0, 3, 1);

        assertEquals(2, coeff1.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testLeftChildCoeffInvalid() {
        WaveletCoefficient level4Coeff = new WaveletCoefficient(0.0, 4, 0);
        WaveletCoefficient level0Coeff = new WaveletCoefficient(0.0, 0, domainStart);

        assertEquals(-1, level4Coeff.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, level0Coeff.getLeftChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testRightChildCoeffLevel1() {
        WaveletCoefficient coeff4 = new WaveletCoefficient(0.0, 1, 4);
        WaveletCoefficient coeff5 = new WaveletCoefficient(0.0, 1, 5);
        WaveletCoefficient coeff6 = new WaveletCoefficient(0.0, 1, 6);
        WaveletCoefficient coeff7 = new WaveletCoefficient(0.0, 1, 7);

        assertEquals(domainStart + 1, coeff4.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 3, coeff5.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 5, coeff6.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(domainStart + 7, coeff7.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testRightChildCoeffLevel2() {
        WaveletCoefficient coeff2 = new WaveletCoefficient(0.0, 2, 2);
        WaveletCoefficient coeff3 = new WaveletCoefficient(0.0, 2, 3);

        assertEquals(5, coeff2.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(7, coeff3.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testRightChildCoeffLevel3() {
        WaveletCoefficient coeff1 = new WaveletCoefficient(0.0, 3, 1);

        assertEquals(3, coeff1.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testRightChildCoeffInvalid() {
        WaveletCoefficient level4Coeff = new WaveletCoefficient(0.0, 4, 0);
        WaveletCoefficient level0Coeff = new WaveletCoefficient(0.0, 0, domainStart);

        assertEquals(-1, level4Coeff.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
        assertEquals(-1, level0Coeff.getRightChildCoeffIndex(domainStart, MAX_LEVEL));
    }

    @Test
    public void testSupportIntervalLevel0() {
        WaveletCoefficient coeff0 = new WaveletCoefficient(0.0, 0, domainStart);
        WaveletCoefficient coeff1 = new WaveletCoefficient(0.0, 0, domainStart + 1);
        WaveletCoefficient coeff2 = new WaveletCoefficient(0.0, 0, domainStart + 2);
        WaveletCoefficient coeff3 = new WaveletCoefficient(0.0, 0, domainStart + 3);
        WaveletCoefficient coeff4 = new WaveletCoefficient(0.0, 0, domainStart + 4);
        WaveletCoefficient coeff5 = new WaveletCoefficient(0.0, 0, domainStart + 5);
        WaveletCoefficient coeff6 = new WaveletCoefficient(0.0, 0, domainStart + 6);
        WaveletCoefficient coeff7 = new WaveletCoefficient(0.0, 0, domainStart + 7);

        assertEquals(new DyadicTupleRange(domainStart, domainStart, 0.0),
                coeff0.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 1, domainStart + 1, 0.0),
                coeff1.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 2, domainStart + 2, 0.0),
                coeff2.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 3, domainStart + 3, 0.0),
                coeff3.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 4, domainStart + 4, 0.0),
                coeff4.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 5, domainStart + 5, 0.0),
                coeff5.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 6, domainStart + 6, 0.0),
                coeff6.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 7, domainStart + 7, 0.0),
                coeff7.convertCoeffToSupportInterval(domainStart, domainEnd));
    }

    @Test
    public void testSupportIntervalLevel1() {
        WaveletCoefficient coeff4 = new WaveletCoefficient(0.0, 1, 4);
        WaveletCoefficient coeff5 = new WaveletCoefficient(0.0, 1, 5);
        WaveletCoefficient coeff6 = new WaveletCoefficient(0.0, 1, 6);
        WaveletCoefficient coeff7 = new WaveletCoefficient(0.0, 1, 7);

        assertEquals(new DyadicTupleRange(domainStart, domainStart + 1, 0.0),
                coeff4.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 2, domainStart + 3, 0.0),
                coeff5.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 4, domainStart + 5, 0.0),
                coeff6.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 6, domainStart + 7, 0.0),
                coeff7.convertCoeffToSupportInterval(domainStart, domainEnd));
    }

    @Test
    public void testSupportIntervalLevel2() {
        WaveletCoefficient coeff2 = new WaveletCoefficient(0.0, 2, 2);
        WaveletCoefficient coeff3 = new WaveletCoefficient(0.0, 2, 3);

        assertEquals(new DyadicTupleRange(domainStart, domainStart + 3, 0.0),
                coeff2.convertCoeffToSupportInterval(domainStart, domainEnd));
        assertEquals(new DyadicTupleRange(domainStart + 4, domainStart + 7, 0.0),
                coeff3.convertCoeffToSupportInterval(domainStart, domainEnd));
    }

    @Test
    public void testSupportIntervalLevel3() {
        WaveletCoefficient coeff1 = new WaveletCoefficient(0.0, 3, 1);

        assertEquals(new DyadicTupleRange(domainStart, domainStart + 7, 0.0),
                coeff1.convertCoeffToSupportInterval(domainStart, domainEnd));
    }
}
