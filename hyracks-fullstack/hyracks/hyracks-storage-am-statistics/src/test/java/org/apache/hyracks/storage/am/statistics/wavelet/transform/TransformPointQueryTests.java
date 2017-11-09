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

import static org.junit.Assert.assertEquals;

public class TransformPointQueryTests extends WaveletTrasformTest {

    public TransformPointQueryTests() {
        super(0, 15, 4, 16, true);
    }

    @Test
    public void IncreasingLevelTestUpperBoarder() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(0l, 4.0), new TransformTuple(8l, 2.0), new TransformTuple(12l, 3.0));
        runTest(initialData);

        Assert.assertEquals(4.0, synopsis.pointQuery(0l), epsilon);
        Assert.assertEquals(2.0, synopsis.pointQuery(8l), epsilon);
        Assert.assertEquals(3.0, synopsis.pointQuery(12l), epsilon);
    }

    @Test
    public void IncreasingLevelTestLowerBoarder() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(7l, 4.0), new TransformTuple(11l, 2.0), new TransformTuple(13l, 3.0));
        runTest(initialData);

        Assert.assertEquals(4.0, synopsis.pointQuery(7l), epsilon);
        Assert.assertEquals(2.0, synopsis.pointQuery(11l), epsilon);
        Assert.assertEquals(3.0, synopsis.pointQuery(13l), epsilon);
    }

    @Test
    public void IncreasingLevelTestMixedBoarder() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(7l, 4.0), new TransformTuple(8l, 2.0), new TransformTuple(13l, 3.0));
        runTest(initialData);

        Assert.assertEquals(4.0, synopsis.pointQuery(7l), epsilon);
        Assert.assertEquals(2.0, synopsis.pointQuery(8l), epsilon);
        Assert.assertEquals(3.0, synopsis.pointQuery(13l), epsilon);
    }

    @Test
    public void DecreasingLevelTest() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(0l, 2.0), new TransformTuple(2l, 4.0), new TransformTuple(4l, 6.0),
                        new TransformTuple(8l, 8.0));
        runTest(initialData);

        Assert.assertEquals(2.0, synopsis.pointQuery(0l), epsilon);
        Assert.assertEquals(4.0, synopsis.pointQuery(2l), epsilon);
        Assert.assertEquals(6.0, synopsis.pointQuery(4l), epsilon);
        Assert.assertEquals(8.0, synopsis.pointQuery(8l), epsilon);
    }

    @Test
    public void MixedLevelTest() throws Exception {
        List<TransformTuple> initialData =
                Arrays.asList(new TransformTuple(7l, 8.0), new TransformTuple(8l, 2.0), new TransformTuple(11l, 4.0),
                        new TransformTuple(15l, 6.0));
        runTest(initialData);

        Assert.assertEquals(8.0, synopsis.pointQuery(7l), epsilon);
        Assert.assertEquals(2.0, synopsis.pointQuery(8l), epsilon);
        Assert.assertEquals(4.0, synopsis.pointQuery(11l), epsilon);
        Assert.assertEquals(6.0, synopsis.pointQuery(15l), epsilon);
    }
}
