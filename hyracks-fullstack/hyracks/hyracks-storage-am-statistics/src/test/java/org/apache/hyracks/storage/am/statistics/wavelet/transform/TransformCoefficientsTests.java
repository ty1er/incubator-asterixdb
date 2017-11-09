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
import java.util.List;

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletCoefficient;
import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformTuple;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransformCoefficientsTests extends WaveletTrasformTest {

    public TransformCoefficientsTests() {
        super(0, 15, 4, 16, false);
    }

    @Test
    public void IncreasingLevelTestUpperBoarder() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(0L, 4.0), new TransformTuple(8L, 2.0), new TransformTuple(12L, 3.0));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(5.75, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals((-1.75), synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals((-1.5), synopsis.findCoeffValue(it, 3L, 3), epsilon);
    }

    @Test
    public void IncreasingLevelTestLowerBoarder() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(7L, 4.0), new TransformTuple(11L, 2.0), new TransformTuple(13L, 3.0));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(3.4375, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals(-2.9375, synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals(-0.5, synopsis.findCoeffValue(it, 2L, 3), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 5L, 2), epsilon);
        Assert.assertEquals(-2, synopsis.findCoeffValue(it, 11L, 1), epsilon);
        Assert.assertEquals(-1.875, synopsis.findCoeffValue(it, 3L, 3), epsilon);
        Assert.assertEquals(-0.5, synopsis.findCoeffValue(it, 6L, 2), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 13L, 1), epsilon);
        Assert.assertEquals(-0.75, synopsis.findCoeffValue(it, 7L, 2), epsilon);
        Assert.assertEquals(-1.5, synopsis.findCoeffValue(it, 14L, 1), epsilon);
    }

    @Test
    public void IncreasingLevelTestMixedBoarder() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(7L, 4.0), new TransformTuple(8L, 2.0), new TransformTuple(13L, 3.0));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(3.8125, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals(-3.3125, synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals(-0.5, synopsis.findCoeffValue(it, 2L, 3), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 5L, 2), epsilon);
        Assert.assertEquals(-2, synopsis.findCoeffValue(it, 11L, 1), epsilon);
        Assert.assertEquals(-1.125, synopsis.findCoeffValue(it, 3L, 3), epsilon);
        Assert.assertEquals(-0.75, synopsis.findCoeffValue(it, 7L, 2), epsilon);
        Assert.assertEquals(-1.5, synopsis.findCoeffValue(it, 14L, 1), epsilon);
    }

    @Test
    public void DecreasingLevelTest() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(0L, 2.0), new TransformTuple(2L, 4.0), new TransformTuple(4L, 6.0),
                        new TransformTuple(8L, 8.0));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(14, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals(-6, synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals(-4, synopsis.findCoeffValue(it, 2L, 3), epsilon);
        Assert.assertEquals(-2, synopsis.findCoeffValue(it, 4L, 2), epsilon);
    }

    @Test
    public void MixedLevelTest() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(7L, 8.0), new TransformTuple(8L, 2.0), new TransformTuple(11L, 4.0),
                        new TransformTuple(15L, 6.0));

        PeekingIterator<WaveletCoefficient> it = runTest(initialData);

        Assert.assertEquals(7.125, synopsis.findCoeffValue(it, 0L, 4), epsilon);
        Assert.assertEquals(-6.125, synopsis.findCoeffValue(it, 1L, 4), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 2L, 3), epsilon);
        Assert.assertEquals(-2, synopsis.findCoeffValue(it, 5L, 2), epsilon);
        Assert.assertEquals(-4, synopsis.findCoeffValue(it, 11L, 1), epsilon);
        Assert.assertEquals(-2.25, synopsis.findCoeffValue(it, 3L, 3), epsilon);
        Assert.assertEquals(-1, synopsis.findCoeffValue(it, 6L, 2), epsilon);
        Assert.assertEquals(-1.5, synopsis.findCoeffValue(it, 7L, 2), epsilon);
        Assert.assertEquals(-3, synopsis.findCoeffValue(it, 15L, 1), epsilon);
    }
}
