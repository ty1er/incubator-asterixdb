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
package org.apache.hyracks.storage.am.lsm.btree;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestDriver;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestNoAntimatterStatisticsFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestStatisticsManager;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestSynopsisElement;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.statistics.common.FieldExtractor;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public abstract class StatisticsTestDriver extends OrderedIndexTestDriver {

    protected final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    public StatisticsTestDriver(BTreeLeafFrameType[] leafFrameTypesToTest) {
        super(leafFrameTypesToTest);
    }

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }

    @Override
    protected OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType, boolean filtered) throws Exception {
        IFieldExtractor[] fieldValueExtractors = new IFieldExtractor[fieldSerdes.length];
        for (int i = 0; i < fieldSerdes.length; i++) {
            fieldValueExtractors[i] = new FieldExtractor(fieldSerdes[i], i, Integer.toString(i));
        }
        return LSMBTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, numKeys,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getMetadataPageManagerFactory(), false, false, false,
                new TestNoAntimatterStatisticsFactory(fieldValueExtractors), harness.getStatisticsManager());
    }

    protected void checkStatistics(TestStatisticsManager statisticsManager, List<CheckTuple> checkTuples,
            int componentNum, int fieldIdx) throws HyracksDataException {
        Collections.sort(checkTuples, (o1, o2) -> o1.getField(fieldIdx).compareTo(o2.getField(fieldIdx)));
        Collection<ISynopsis> stats = statisticsManager.getStatistics(Integer.toString(fieldIdx));
        Assert.assertEquals(componentNum, stats.size());
        ISynopsis mergedSynopsis = null;
        for (ISynopsis s : stats) {
            Assert.assertTrue(statisticsManager.isFlushed(s));
            if (mergedSynopsis == null) {
                mergedSynopsis = s;
                continue;
            }
            mergedSynopsis.merge(s);
        }
        Iterator<TestSynopsisElement> synopsisIt = mergedSynopsis.getElements().iterator();
        Object prevKey = null;
        int keyTuples = 0;
        for (CheckTuple c : checkTuples) {
            Object key = c.getField(fieldIdx);
            if (prevKey == null || key.equals(prevKey)) {
                keyTuples++;
                prevKey = key;
                continue;
            }
            Assert.assertTrue(synopsisIt.hasNext());
            TestSynopsisElement e = synopsisIt.next();
            Assert.assertEquals(prevKey, e.getKey());
            Assert.assertEquals(keyTuples, e.getValue(), 0.0001);
            keyTuples = 1;
            prevKey = key;
        }
        //check last tuple
        TestSynopsisElement last = synopsisIt.next();
        Assert.assertEquals(prevKey, last.getKey());
        Assert.assertEquals(keyTuples, last.getValue(), 0.0001);

        Assert.assertFalse(synopsisIt.hasNext());
    }
}