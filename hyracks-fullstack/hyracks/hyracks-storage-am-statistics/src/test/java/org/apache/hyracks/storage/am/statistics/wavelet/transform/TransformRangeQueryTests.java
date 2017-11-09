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

import org.apache.hyracks.storage.am.statistics.wavelet.helper.TransformTuple;
import org.junit.Assert;
import org.junit.Test;

public class TransformRangeQueryTests extends WaveletTransformTest {

    public TransformRangeQueryTests() {
        super(0, 15, 4, 16, true);
    }

    @Test
    public void EmptyPointTest() throws Exception {
        List<TransformTuple> initialData = Arrays.asList();
        runTest(initialData);

        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 0), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 1), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 2), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 3), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 4), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 5), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 6), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 7), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 8), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 9), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 10), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 11), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 12), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 13), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 14), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 15), epsilon);
    }

    @Test
    public void SinglePointDomainStartTest() throws Exception {
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(0, 4));
        runTest(initialData);

        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 1), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 2), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 3), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 4), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 5), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 6), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 7), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 8), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 9), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 11), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 12), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 13), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 14), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 15), epsilon);

        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 1), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 2), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 3), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 4), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 5), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 6), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 7), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 8), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 9), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 10), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 11), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 12), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 13), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 14), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(1, 15), epsilon);
    }

    @Test
    public void SinglePointDomainEndTest() throws Exception {
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(15, 4));
        runTest(initialData);

        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 1), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 2), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 3), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 4), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 5), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 6), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 7), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 8), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 9), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 10), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 11), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 12), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 13), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 14), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 15), epsilon);

        Assert.assertEquals(4.0, synopsis.rangeQuery(1, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(2, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(3, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(4, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(5, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(6, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(7, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(8, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(9, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(10, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(11, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(12, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(13, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(14, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(15, 15), epsilon);
    }

    @Test
    public void TwoPointsTest() throws Exception {
        List<TransformTuple> initialData = Arrays.asList(new TransformTuple(7, 4), new TransformTuple(11, 2));
        runTest(initialData);

        Assert.assertEquals(0.0, synopsis.rangeQuery(0, 6), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(8, 10), epsilon);
        Assert.assertEquals(0.0, synopsis.rangeQuery(12, 15), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(6, 8), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(5, 9), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(4, 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(3, 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(2, 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(1, 10), epsilon);
        Assert.assertEquals(4.0, synopsis.rangeQuery(0, 10), epsilon);
        Assert.assertEquals(2.0, synopsis.rangeQuery(8, 15), epsilon);
        Assert.assertEquals(2.0, synopsis.rangeQuery(9, 14), epsilon);
        Assert.assertEquals(2.0, synopsis.rangeQuery(10, 13), epsilon);
        Assert.assertEquals(2.0, synopsis.rangeQuery(11, 12), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(7, 11), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(6, 12), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(5, 13), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(4, 14), epsilon);
        Assert.assertEquals(6.0, synopsis.rangeQuery(3, 15), epsilon);
    }
}
