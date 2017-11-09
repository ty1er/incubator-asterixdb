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
package org.apache.hyracks.storage.am.statistics.wavelet.transform;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformTuple;
import org.apache.hyracks.test.support.RepeatRule;
import org.apache.hyracks.test.support.RepeatRule.Repeat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TransformBorderValuesTests extends WaveletTransformTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { Long.MIN_VALUE, Long.MAX_VALUE, Long.SIZE },
                { Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.SIZE },
                { Short.MIN_VALUE, Short.MAX_VALUE, Short.SIZE }, { Byte.MIN_VALUE, Byte.MAX_VALUE, Byte.SIZE } });
    }

    public TransformBorderValuesTests(long domainStart, long domainEnd, int maxLevel) {
        super(domainStart, domainEnd, maxLevel, 300, true);
    }

    @Test
    public void DomainStartTest() throws Exception {
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(domainStart, 100));
        runTest(initialData);

        Assert.assertEquals(100.0, synopsis.rangeQuery(domainStart, domainStart + 1), epsilon);
        Assert.assertEquals(100.0, synopsis.rangeQuery(domainStart, 0), epsilon);
        Assert.assertEquals(100.0, synopsis.rangeQuery(domainStart, domainEnd), epsilon);
    }

    @Test
    public void DomainEndTest() throws Exception {
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(domainEnd, 100));
        runTest(initialData);

        Assert.assertEquals(100.0, synopsis.rangeQuery(domainEnd - 1, domainEnd), epsilon);
        Assert.assertEquals(100.0, synopsis.rangeQuery(0, domainEnd), epsilon);
        Assert.assertEquals(100.0, synopsis.rangeQuery(domainStart, domainEnd), epsilon);
    }

    @Test
    public void MixedTest() throws Exception {
        long rand = ThreadLocalRandom.current().nextLong(domainStart + 1, domainEnd - 1);
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(domainStart, 100), new TransformTuple(rand, 100),
                        new TransformTuple(domainEnd, 100));
        runTest(initialData);

        Assert.assertEquals(100.0, synopsis.rangeQuery(domainStart, rand - 1), epsilon);
        Assert.assertEquals(100.0, synopsis.rangeQuery(domainStart + 1, rand), epsilon);
        Assert.assertEquals(200.0, synopsis.rangeQuery(domainStart, rand), epsilon);
        Assert.assertEquals(200.0, synopsis.rangeQuery(rand, domainEnd), epsilon);
        Assert.assertEquals(100.0, synopsis.rangeQuery(rand + 1, domainEnd), epsilon);
        Assert.assertEquals(100.0, synopsis.rangeQuery(rand, domainEnd - 1), epsilon);
        Assert.assertEquals(300.0, synopsis.rangeQuery(domainStart, domainEnd), epsilon);
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Test
    @Repeat(times = 1000)
    public void RandTest() throws Exception {
        long rand1 = ThreadLocalRandom.current().nextLong(domainStart + 1, domainEnd - 1);
        long rand2 = ThreadLocalRandom.current().nextLong(domainStart, domainEnd);
        long rand3 = ThreadLocalRandom.current().nextLong(domainStart, domainEnd);

        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(rand1, 100));
        runTest(initialData);

        Assert.assertEquals(0.0, synopsis.rangeQuery(domainStart, rand1 - 1), epsilon);
        Assert.assertEquals(100.0, synopsis.rangeQuery(domainStart, rand1), epsilon);
        Assert.assertEquals(100.0, synopsis.rangeQuery(rand1, domainEnd), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(rand1 + 1, domainEnd), epsilon);

        long rangeStart = Math.min(rand2, rand3);
        long rangeEnd = Math.min(rand2, rand3);
        Assert.assertEquals(rand1 >= rangeStart && rand1 <= rangeEnd ? 100.0 : 0.0, synopsis.rangeQuery(rangeStart, rangeEnd),
                epsilon);
    }

    //    @Test
    //    public void DecreasingLevelTest() throws Exception {
    //        List<TransformTuple> initialData =
    //                Arrays.asList(new TransformTuple(Long.MIN_VALUE, 64.0), new TransformTuple(Integer.MIN_VALUE, 32.0),
    //                        new TransformTuple(Short.MIN_VALUE, 16.0), new TransformTuple(Byte.MIN_VALUE, 8.0));
    //        runTest(initialData);
    //
    //        assertEquals(64.0, synopsis.rangeQuery(Long.MIN_VALUE, Long.MIN_VALUE + 1), epsilon);
    //        assertEquals(32.0, synopsis.rangeQuery(Integer.MIN_VALUE, Integer.MIN_VALUE + 1), epsilon);
    //        assertEquals(16.0, synopsis.rangeQuery(Short.MIN_VALUE, Short.MIN_VALUE + 1), epsilon);
    //        assertEquals(8.0, synopsis.rangeQuery(Byte.MIN_VALUE, Byte.MIN_VALUE + 1), epsilon);
    //    }
    //
    //    @Test
    //    public void MixedLevelTest() throws Exception {
    //        List<TransformTuple> initialData =
    //                Arrays.asList(new TransformTuple(Integer.MIN_VALUE, 32.0), new TransformTuple(-1l, 1.0),
    //                        new TransformTuple(0l, 1.0), new TransformTuple(Byte.MAX_VALUE, 8.0),
    //                        new TransformTuple(Long.MAX_VALUE, 64.0));
    //        runTest(initialData);
    //
    //    }
    //
    //    @Test
    //    public void IncreasingLevelTestLowerBoarder() throws Exception {
    //        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(Integer.MIN_VALUE - 1l, 32.0),
    //                new TransformTuple(Byte.MIN_VALUE - 1l, 8.0), new TransformTuple(-1l, 3.0),
    //                new TransformTuple(Short.MAX_VALUE, 16.0), new TransformTuple(Long.MAX_VALUE, 64.0));
    //        runTest(initialData);
    //    }
    //
    //    @Test
    //    public void IncreasingLevelTestUpperBoarder() throws Exception {
    //        List<TransformTuple> initialData =
    //                Arrays.asList(new TransformTuple(Long.MIN_VALUE, 64.0), new TransformTuple(Short.MIN_VALUE, 16.0),
    //                        new TransformTuple(Byte.MAX_VALUE + 1l, 1.0), new TransformTuple(Integer.MAX_VALUE + 1l, 32.0));
    //        runTest(initialData);
    //    }

}
