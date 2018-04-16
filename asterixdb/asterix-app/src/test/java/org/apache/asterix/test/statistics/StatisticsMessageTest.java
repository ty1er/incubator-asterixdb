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
package org.apache.asterix.test.statistics;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.bootstrap.TestNodeController.SecondaryIndexInfo;
import org.apache.asterix.app.data.gen.TupleGenerator;
import org.apache.asterix.app.data.gen.TupleGenerator.GenerationFunction;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.LSMPrimaryUpsertOperatorNodePushable;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.test.dataflow.StorageTestUtils;
import org.apache.asterix.test.dataflow.TestCountingStatisticsFactory.CountingSynopsis;
import org.apache.asterix.test.dataflow.TestDataset;
import org.apache.asterix.test.statistics.TestStatisticsMessageBroker.TestStatisticsMessageEntry;
import org.apache.asterix.test.statistics.TestStatisticsMessageBroker.TestStatisticsMessageID;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.nc.application.NCServiceContext;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.am.statistics.common.ComponentStatisticsId;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.test.support.TestStorageManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class StatisticsMessageTest {
    private static TestNodeController nc;
    private static TestStatisticsMessageBroker msgBroker;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static IHyracksTaskContext ctx;
    private static ITransactionContext txnCtx;
    private static TestLsmBtree primaryLsmBtree;
    private static TestLsmBtree secondaryLsmBtree;
    private static IIndexDataflowHelper[] indexDataflowHelpers = new IIndexDataflowHelper[2];
    private static IStorageManager storageManager;
    private static final int PARTITION = 0;
    private static LSMInsertDeleteOperatorNodePushable insertOp;
    private static LSMInsertDeleteOperatorNodePushable deleteOp;
    private static LSMPrimaryUpsertOperatorNodePushable upsertOp;
    private static int NUM_INSERT_RECORDS = 1000;
    private static TupleGenerator insertOpTupleGenerator;
    private static TupleGenerator deleteOpTupleGenerator;
    private static final String KEY_FIELD_NAME = "key";
    private static final String INDEXED_FIELD_NAME = "indexed_value";
    private static final IAType KEY_FIELD_TYPE = BuiltinType.AINT32;
    private static final IAType INDEXED_FIELD_TYPE = BuiltinType.AINT64;
    private static final String[] FIELD_NAMES = new String[] { KEY_FIELD_NAME, INDEXED_FIELD_NAME };
    private static final IAType[] FIELD_TYPES = new IAType[] { KEY_FIELD_TYPE, INDEXED_FIELD_TYPE };
    private static final ARecordType RECORD_TYPE = new ARecordType("TestRecordType", FIELD_NAMES, FIELD_TYPES, false);

    private static final String INDEX_NAME = "TestIdx";
    private static final List<List<String>> INDEX_FIELD_NAMES = Arrays.asList(Arrays.asList(INDEXED_FIELD_NAME));
    private static final List<IAType> INDEX_FIELD_TYPES = Arrays.asList(INDEXED_FIELD_TYPE);
    private static final List<Integer> INDEX_FIELD_INDICATORS = Arrays.asList(Index.RECORD_INDICATOR);

    private static final TestDataset STATS_DATASET = new TestDataset(StorageTestUtils.DATAVERSE_NAME,
            StorageTestUtils.DATASET_NAME, StorageTestUtils.DATAVERSE_NAME, StorageTestUtils.DATA_TYPE_NAME,
            StorageTestUtils.NODE_GROUP_NAME, NoMergePolicyFactory.NAME, null,
            new InternalDatasetDetails(null, PartitioningStrategy.HASH, StorageTestUtils.PARTITIONING_KEYS, null, null,
                    null, false, null),
            new HashMap<>(), DatasetType.INTERNAL, StorageTestUtils.DATASET_ID, 0, true, true);
    private static final TestStatisticsMessageID keyFieldStatisticsID =
            new TestStatisticsMessageID(StorageTestUtils.DATAVERSE_NAME, StorageTestUtils.DATASET_NAME,
                    StorageTestUtils.DATASET_NAME, KEY_FIELD_NAME, "asterix_nc1", "partition_0", false);
    private static final TestStatisticsMessageID keyFieldAntimatterStatisticsID =
            new TestStatisticsMessageID(StorageTestUtils.DATAVERSE_NAME, StorageTestUtils.DATASET_NAME,
                    StorageTestUtils.DATASET_NAME, KEY_FIELD_NAME, "asterix_nc1", "partition_0", true);
    private static final TestStatisticsMessageID indexedFieldStatisticsID =
            new TestStatisticsMessageID(StorageTestUtils.DATAVERSE_NAME, StorageTestUtils.DATASET_NAME, INDEX_NAME,
                    INDEXED_FIELD_NAME, "asterix_nc1", "partition_0", false);
    private static final TestStatisticsMessageID indexedFieldAntimatterStatisticsID =
            new TestStatisticsMessageID(StorageTestUtils.DATAVERSE_NAME, StorageTestUtils.DATASET_NAME, INDEX_NAME,
                    INDEXED_FIELD_NAME, "asterix_nc1", "partition_0", true);

    @BeforeClass
    public static void startTestCluster() throws Exception {
        TestHelper.deleteExistingInstanceFiles();
        String configPath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources", "cc.conf").toString();
        nc = new TestNodeController(configPath, false);
        nc.init();
        NCAppRuntimeContext ncAppCtx = nc.getAppRuntimeContext();
        NCServiceContext ncCtx = (NCServiceContext) ncAppCtx.getServiceContext();
        msgBroker = new TestStatisticsMessageBroker();
        ncCtx.setMessageBroker(msgBroker);
        dsLifecycleMgr = ncAppCtx.getDatasetLifecycleManager();
        storageManager = new TestStorageManager() {
            @Override
            public IResourceLifecycleManager<IIndex> getLifecycleManager(INCServiceContext ctx) {
                return dsLifecycleMgr;
            }

            @Override
            public ILocalResourceRepository getLocalResourceRepository(INCServiceContext ctx) {
                return ncAppCtx.getLocalResourceRepository();
            }
        };
    }

    @AfterClass
    public static void stopTestCluster() throws Exception {
        nc.deInit();
        TestHelper.deleteExistingInstanceFiles();
    }

    @Before
    public void setUp() throws Exception {
        insertOpTupleGenerator = new TupleGenerator(RECORD_TYPE, StorageTestUtils.META_TYPE,
                StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS,
                new GenerationFunction[] { GenerationFunction.INCREASING, GenerationFunction.INCREASING },
                StorageTestUtils.UNIQUE_RECORD_FIELDS, StorageTestUtils.META_GEN_FUNCTION,
                StorageTestUtils.UNIQUE_META_FIELDS);
        //StorageTestUtils.getTupleGenerator();
        deleteOpTupleGenerator = new TupleGenerator(RECORD_TYPE, StorageTestUtils.META_TYPE,
                StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS,
                new GenerationFunction[] { GenerationFunction.INCREASING, GenerationFunction.INCREASING },
                StorageTestUtils.UNIQUE_RECORD_FIELDS, StorageTestUtils.META_GEN_FUNCTION,
                StorageTestUtils.UNIQUE_META_FIELDS);
        createIndex();
        msgBroker.getStatsMessages().clear();
    }

    private void createIndex() throws Exception {
        StorageComponentProvider storageProvider = new StorageComponentProvider() {
            @Override
            public IStorageManager getStorageManager() {
                return storageManager;
            }
        };
        PrimaryIndexInfo primaryIndexInfo = nc.createPrimaryIndex(STATS_DATASET, StorageTestUtils.KEY_TYPES,
                RECORD_TYPE, StorageTestUtils.META_TYPE, null, storageProvider, StorageTestUtils.KEY_INDEXES,
                StorageTestUtils.KEY_INDICATORS_LIST, PARTITION);
        Index secondaryIndex = new Index(StorageTestUtils.DATAVERSE_NAME, StorageTestUtils.DATASET_NAME, INDEX_NAME,
                IndexType.BTREE, INDEX_FIELD_NAMES, INDEX_FIELD_INDICATORS, INDEX_FIELD_TYPES, false, false, false, 0);
        SecondaryIndexInfo secondaryIndexInfo =
                nc.createSecondaryIndex(primaryIndexInfo, secondaryIndex, storageProvider, 0);
        IndexDataflowHelperFactory[] idxHelperFactories = new IndexDataflowHelperFactory[] {
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider()),
                new IndexDataflowHelperFactory(nc.getStorageManager(), secondaryIndexInfo.getFileSplitProvider()) };
        JobId jobId = nc.newJobId();
        ctx = nc.createTestContext(jobId, PARTITION, false);
        txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(jobId),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
        for (int i = 0; i < idxHelperFactories.length; i++) {
            indexDataflowHelpers[i] =
                    idxHelperFactories[i].create(ctx.getJobletContext().getServiceContext(), PARTITION);
            indexDataflowHelpers[i].open();
            TestLsmBtree tree = (TestLsmBtree) indexDataflowHelpers[i].getIndexInstance();
            if (i == 0) {
                primaryLsmBtree = tree;
            } else {
                secondaryLsmBtree = tree;
            }
            indexDataflowHelpers[i].close();
            StorageTestUtils.allowAllOps(tree);
        }

        insertOp = nc.getInsertPipeline(ctx, STATS_DATASET, StorageTestUtils.KEY_TYPES, RECORD_TYPE,
                StorageTestUtils.META_TYPE, null, StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS_LIST,
                storageProvider, secondaryIndex).getLeft();
        deleteOp = nc
                .getInsertPipeline(ctx, STATS_DATASET, StorageTestUtils.KEY_TYPES, RECORD_TYPE,
                        StorageTestUtils.META_TYPE, null, StorageTestUtils.KEY_INDEXES,
                        StorageTestUtils.KEY_INDICATORS_LIST, storageProvider, secondaryIndex, IndexOperation.DELETE)
                .getLeft();
        upsertOp = nc.getUpsertPipeline(ctx, STATS_DATASET, StorageTestUtils.KEY_TYPES, RECORD_TYPE,
                StorageTestUtils.META_TYPE, null, StorageTestUtils.KEY_INDEXES, StorageTestUtils.KEY_INDICATORS_LIST,
                storageProvider, null, true).getLeft();
    }

    //TODO: add tests for non-indexed fields

    @After
    public void tearDown() throws Exception {
        for (int i = 0; i < indexDataflowHelpers.length; i++) {
            indexDataflowHelpers[i].destroy();
        }
    }

    private void insertRecords(int numRecords) throws Exception {
        ingestRecords(numRecords, insertOp, insertOpTupleGenerator);
    }

    private void deleteRecords(int numRecords) throws Exception {
        ingestRecords(numRecords, deleteOp, deleteOpTupleGenerator);
    }

    private void upsertRecords(int numRecords) throws Exception {
        ingestRecords(numRecords, upsertOp, deleteOpTupleGenerator);
    }

    private void ingestRecords(int numRecords, IndexInsertUpdateDeleteOperatorNodePushable opNodePushable,
            TupleGenerator tupleGenerator) throws Exception {
        opNodePushable.open();
        VSizeFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);
        ITupleReference tuple;
        for (int i = 0; i < numRecords; i++) {
            tuple = tupleGenerator.next();
            DataflowUtils.addTupleToFrame(tupleAppender, tuple, opNodePushable);
        }
        if (tupleAppender.getTupleCount() > 0) {
            tupleAppender.write(opNodePushable, true);
        }
        opNodePushable.close();
    }

    @Test
    public void testFlushEmptyComponentStatistics() throws Exception {
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        // flush statistics without inserting any record
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsMessageEntry> keyFieldStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> keyFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldAntimatterStatisticsID);

        Assert.assertNull(keyFieldStatsEntries);
        Assert.assertNull(valueFieldStatsEntries);
        Assert.assertNull(keyFieldAntimatterStatsEntries);
        Assert.assertNull(valueFieldAntimatterStatsEntries);
    }

    @Test
    public void testFlushStatistics() throws Exception {
        insertRecords(NUM_INSERT_RECORDS);
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsMessageEntry> keyFieldStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> keyFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldAntimatterStatisticsID);

        Assert.assertEquals(1, keyFieldStatsEntries.size());
        CountingSynopsis synopsis = (CountingSynopsis) keyFieldStatsEntries.iterator().next().getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        Assert.assertEquals(1, valueFieldStatsEntries.size());
        synopsis = (CountingSynopsis) valueFieldStatsEntries.iterator().next().getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        ComponentStatisticsId firstKeyComponentId = keyFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstKeyComponentId.getMinTimestamp(), firstKeyComponentId.getMaxTimestamp());
        ComponentStatisticsId firstValueComponentId = valueFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstValueComponentId.getMinTimestamp(), firstValueComponentId.getMaxTimestamp());
        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        Assert.assertNull(keyFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());
        Assert.assertNull(valueFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(firstKeyComponentId, keyFieldAntimatterStatsEntries.iterator().next().getComponentId());
        Assert.assertEquals(firstValueComponentId, valueFieldAntimatterStatsEntries.iterator().next().getComponentId());
    }

    @Test
    public void testFlushStatisticsWithAntimatter() throws Exception {
        // insert 500 records...
        insertRecords(NUM_INSERT_RECORDS / 2);
        // and delete 1000 records right after, generating 500 antimatter entries
        deleteRecords(NUM_INSERT_RECORDS);
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());

        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsMessageEntry> keyFieldStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> keyFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldAntimatterStatisticsID);

        Assert.assertEquals(1, keyFieldStatsEntries.size());
        Assert.assertNull(keyFieldStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldStatsEntries.size());
        Assert.assertNull(valueFieldStatsEntries.iterator().next().getSynopsis());
        ComponentStatisticsId firstKeyComponentId = keyFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstKeyComponentId.getMinTimestamp(), firstKeyComponentId.getMaxTimestamp());
        ComponentStatisticsId firstValueComponentId = valueFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstValueComponentId.getMinTimestamp(), firstValueComponentId.getMaxTimestamp());
        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        CountingSynopsis synopsis = (CountingSynopsis) keyFieldAntimatterStatsEntries.iterator().next().getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());
        synopsis = (CountingSynopsis) valueFieldAntimatterStatsEntries.iterator().next().getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
        Assert.assertEquals(firstKeyComponentId, keyFieldAntimatterStatsEntries.iterator().next().getComponentId());
        Assert.assertEquals(firstValueComponentId, valueFieldAntimatterStatsEntries.iterator().next().getComponentId());
    }

    @Test
    public void testFlushDeletedComponentStatistics() throws Exception {
        // insert 1000 records...
        insertRecords(NUM_INSERT_RECORDS);
        // ...and delete them right after
        deleteRecords(NUM_INSERT_RECORDS);
        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());

        // flush statistics after deletion all of the records
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsMessageEntry> keyFieldStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> keyFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldAntimatterStatisticsID);

        Assert.assertEquals(1, keyFieldStatsEntries.size());
        Assert.assertNull(keyFieldStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldStatsEntries.size());
        Assert.assertNull(valueFieldStatsEntries.iterator().next().getSynopsis());
        ComponentStatisticsId firstKeyComponentId = keyFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstKeyComponentId.getMinTimestamp(), firstKeyComponentId.getMaxTimestamp());
        ComponentStatisticsId firstValueComponentId = valueFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstValueComponentId.getMinTimestamp(), firstValueComponentId.getMaxTimestamp());
        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        Assert.assertNull(keyFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());
        Assert.assertNull(valueFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(firstKeyComponentId, keyFieldAntimatterStatsEntries.iterator().next().getComponentId());
        Assert.assertEquals(firstValueComponentId, valueFieldAntimatterStatsEntries.iterator().next().getComponentId());
    }

    @Test
    public void testMergeStatisticsReconcileAll() throws Exception {
        // insert 1000 records...
        insertRecords(NUM_INSERT_RECORDS);
        // flush and generate statistics about 1000 records...
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsMessageEntry> keyFieldStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> keyFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldAntimatterStatisticsID);

        Assert.assertEquals(1, keyFieldStatsEntries.size());
        CountingSynopsis synopsis = (CountingSynopsis) keyFieldStatsEntries.iterator().next().getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        Assert.assertEquals(1, valueFieldStatsEntries.size());
        synopsis = (CountingSynopsis) valueFieldStatsEntries.iterator().next().getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        ComponentStatisticsId firstKeyComponentId = keyFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstKeyComponentId.getMinTimestamp(), firstKeyComponentId.getMaxTimestamp());
        ComponentStatisticsId firstValueComponentId = valueFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstValueComponentId.getMinTimestamp(), firstValueComponentId.getMaxTimestamp());
        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        Assert.assertNull(keyFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());
        Assert.assertNull(valueFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(firstKeyComponentId, keyFieldAntimatterStatsEntries.iterator().next().getComponentId());
        Assert.assertEquals(firstValueComponentId, valueFieldAntimatterStatsEntries.iterator().next().getComponentId());

        // ... and delete all 1000 records
        deleteRecords(NUM_INSERT_RECORDS);
        //accessors.scheduleFlush(lsmBtree.getIOOperationCallback());
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Assert.assertEquals(2, keyFieldStatsEntries.size());
        Assert.assertNull(Iterators.get(keyFieldStatsEntries.iterator(), 1).getSynopsis());
        Assert.assertEquals(2, valueFieldStatsEntries.size());
        Assert.assertNull(Iterators.get(valueFieldStatsEntries.iterator(), 1).getSynopsis());
        ComponentStatisticsId secondKeyComponentId = Iterators.get(keyFieldStatsEntries.iterator(), 1).getComponentId();
        Assert.assertEquals(secondKeyComponentId.getMinTimestamp(), secondKeyComponentId.getMaxTimestamp());
        ComponentStatisticsId secondValueComponentId =
                Iterators.get(valueFieldStatsEntries.iterator(), 1).getComponentId();
        Assert.assertEquals(secondValueComponentId.getMinTimestamp(), secondValueComponentId.getMaxTimestamp());
        Assert.assertEquals(2, keyFieldAntimatterStatsEntries.size());
        synopsis = (CountingSynopsis) Iterators.get(keyFieldAntimatterStatsEntries.iterator(), 1).getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        Assert.assertEquals(2, valueFieldAntimatterStatsEntries.size());
        synopsis = (CountingSynopsis) Iterators.get(valueFieldAntimatterStatsEntries.iterator(), 1).getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        Assert.assertEquals(secondKeyComponentId,
                Iterators.get(keyFieldAntimatterStatsEntries.iterator(), 1).getComponentId());
        Assert.assertEquals(secondValueComponentId,
                Iterators.get(valueFieldAntimatterStatsEntries.iterator(), 1).getComponentId());

        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
        // trigger merge and reconciliation resulting in reconciliation of all records
        StorageTestUtils.fullMerge(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET);
        StorageTestUtils.fullMerge(dsLifecycleMgr, secondaryLsmBtree, STATS_DATASET);

        Assert.assertEquals(1, keyFieldStatsEntries.size());
        Assert.assertNull(keyFieldStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldStatsEntries.size());
        Assert.assertNull(valueFieldStatsEntries.iterator().next().getSynopsis());
        ComponentStatisticsId mergedKeyComponentId = keyFieldStatsEntries.iterator().next().getComponentId();
        ComponentStatisticsId mergedValueComponentId = valueFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        Assert.assertNull(keyFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());
        Assert.assertNull(valueFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(mergedKeyComponentId, keyFieldAntimatterStatsEntries.iterator().next().getComponentId());
        Assert.assertEquals(mergedValueComponentId,
                valueFieldAntimatterStatsEntries.iterator().next().getComponentId());

        Assert.assertEquals(firstKeyComponentId.getMinTimestamp(), mergedKeyComponentId.getMinTimestamp());
        Assert.assertEquals(firstValueComponentId.getMinTimestamp(), mergedValueComponentId.getMinTimestamp());
        Assert.assertEquals(secondKeyComponentId.getMaxTimestamp(), mergedKeyComponentId.getMaxTimestamp());
        Assert.assertEquals(secondValueComponentId.getMaxTimestamp(), mergedValueComponentId.getMaxTimestamp());
    }

    @Test
    public void testMergeStatisticsExcessAntimatter() throws Exception {
        // create a first component with records [1;500]
        insertRecords(NUM_INSERT_RECORDS / 2);
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);

        Collection<TestStatisticsMessageEntry> keyFieldStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldStatisticsID);
        Collection<TestStatisticsMessageEntry> keyFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(keyFieldAntimatterStatisticsID);
        Collection<TestStatisticsMessageEntry> valueFieldAntimatterStatsEntries =
                msgBroker.getStatsMessages().get(indexedFieldAntimatterStatisticsID);

        Assert.assertEquals(1, keyFieldStatsEntries.size());
        CountingSynopsis synopsis = (CountingSynopsis) keyFieldStatsEntries.iterator().next().getSynopsis();
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(1, valueFieldStatsEntries.size());
        synopsis = (CountingSynopsis) valueFieldStatsEntries.iterator().next().getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
        ComponentStatisticsId firstKeyComponentId = keyFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstKeyComponentId.getMinTimestamp(), firstKeyComponentId.getMaxTimestamp());
        ComponentStatisticsId firstValueComponentId = valueFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(firstValueComponentId.getMinTimestamp(), firstValueComponentId.getMaxTimestamp());
        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        Assert.assertNull(keyFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());
        Assert.assertNull(valueFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(firstKeyComponentId, keyFieldAntimatterStatsEntries.iterator().next().getComponentId());
        Assert.assertEquals(firstValueComponentId, valueFieldAntimatterStatsEntries.iterator().next().getComponentId());

        // insert records [501;1000]
        insertRecords(NUM_INSERT_RECORDS / 2);
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);
        ILSMDiskComponent primaryC2 = primaryLsmBtree.getDiskComponents().get(0);
        ILSMDiskComponent secondaryC2 = secondaryLsmBtree.getDiskComponents().get(0);

        Assert.assertEquals(2, keyFieldStatsEntries.size());
        synopsis = (CountingSynopsis) Iterators.get(keyFieldStatsEntries.iterator(), 1).getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
        Assert.assertEquals(2, valueFieldStatsEntries.size());
        synopsis = (CountingSynopsis) Iterators.get(valueFieldStatsEntries.iterator(), 1).getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS / 2, synopsis.getCount());
        ComponentStatisticsId secondKeyComponentId = Iterators.get(keyFieldStatsEntries.iterator(), 1).getComponentId();
        Assert.assertEquals(secondKeyComponentId.getMinTimestamp(), secondKeyComponentId.getMaxTimestamp());
        ComponentStatisticsId secondValueComponentId =
                Iterators.get(valueFieldStatsEntries.iterator(), 1).getComponentId();
        Assert.assertEquals(secondValueComponentId.getMinTimestamp(), secondValueComponentId.getMaxTimestamp());
        Assert.assertEquals(2, keyFieldAntimatterStatsEntries.size());
        Assert.assertNull(Iterators.get(keyFieldAntimatterStatsEntries.iterator(), 1).getSynopsis());
        Assert.assertEquals(2, valueFieldAntimatterStatsEntries.size());
        Assert.assertNull(Iterators.get(valueFieldAntimatterStatsEntries.iterator(), 1).getSynopsis());
        Assert.assertEquals(secondKeyComponentId,
                Iterators.get(keyFieldAntimatterStatsEntries.iterator(), 1).getComponentId());
        Assert.assertEquals(secondValueComponentId,
                Iterators.get(valueFieldAntimatterStatsEntries.iterator(), 1).getComponentId());

        // delete records [1;1000]
        deleteRecords(NUM_INSERT_RECORDS);
        StorageTestUtils.flush(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET, false);
        ILSMDiskComponent primaryC3 = primaryLsmBtree.getDiskComponents().get(0);
        ILSMDiskComponent secondaryC3 = secondaryLsmBtree.getDiskComponents().get(0);

        Assert.assertEquals(3, keyFieldStatsEntries.size());
        Assert.assertNull(Iterators.get(keyFieldStatsEntries.iterator(), 2).getSynopsis());
        Assert.assertEquals(3, valueFieldStatsEntries.size());
        Assert.assertNull(Iterators.get(valueFieldStatsEntries.iterator(), 2).getSynopsis());
        ComponentStatisticsId thirdKeyComponentId = Iterators.get(keyFieldStatsEntries.iterator(), 2).getComponentId();
        Assert.assertEquals(thirdKeyComponentId.getMinTimestamp(), thirdKeyComponentId.getMaxTimestamp());
        ComponentStatisticsId thirdValueComponentId =
                Iterators.get(valueFieldStatsEntries.iterator(), 2).getComponentId();
        Assert.assertEquals(thirdValueComponentId.getMinTimestamp(), thirdValueComponentId.getMaxTimestamp());
        Assert.assertEquals(3, keyFieldAntimatterStatsEntries.size());
        synopsis = (CountingSynopsis) Iterators.get(keyFieldAntimatterStatsEntries.iterator(), 2).getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        Assert.assertEquals(3, valueFieldAntimatterStatsEntries.size());
        synopsis = (CountingSynopsis) Iterators.get(valueFieldAntimatterStatsEntries.iterator(), 2).getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        Assert.assertEquals(thirdKeyComponentId,
                Iterators.get(keyFieldAntimatterStatsEntries.iterator(), 2).getComponentId());
        Assert.assertEquals(thirdValueComponentId,
                Iterators.get(valueFieldAntimatterStatsEntries.iterator(), 2).getComponentId());

        nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());

        // trigger merge and reconciliation between records in first and last components
        StorageTestUtils.merge(Arrays.asList(primaryC3, primaryC2), dsLifecycleMgr, primaryLsmBtree, STATS_DATASET);
        StorageTestUtils.merge(Arrays.asList(secondaryC3, secondaryC2), dsLifecycleMgr, secondaryLsmBtree,
                STATS_DATASET);

        Assert.assertEquals(2, keyFieldStatsEntries.size());
        Assert.assertNull(Iterators.get(keyFieldStatsEntries.iterator(), 1).getSynopsis());
        Assert.assertEquals(2, valueFieldStatsEntries.size());
        Assert.assertNull(Iterators.get(valueFieldStatsEntries.iterator(), 1).getSynopsis());
        ComponentStatisticsId mergedKeyComponentId = Iterators.get(keyFieldStatsEntries.iterator(), 1).getComponentId();
        ComponentStatisticsId mergedValueComponentId =
                Iterators.get(valueFieldStatsEntries.iterator(), 1).getComponentId();
        Assert.assertEquals(2, keyFieldAntimatterStatsEntries.size());
        synopsis = (CountingSynopsis) Iterators.get(keyFieldAntimatterStatsEntries.iterator(), 1).getSynopsis();
        Assert.assertNotNull(synopsis);
        //synopsis has NUM_INSERT_RECORDS antimatter records because all antimatter is kept during the merge
        // TODO: this is a bug, records needs to be traced
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        Assert.assertEquals(2, valueFieldAntimatterStatsEntries.size());
        synopsis = (CountingSynopsis) Iterators.get(valueFieldAntimatterStatsEntries.iterator(), 1).getSynopsis();
        Assert.assertNotNull(synopsis);
        Assert.assertEquals(NUM_INSERT_RECORDS, synopsis.getCount());
        Assert.assertEquals(mergedKeyComponentId,
                Iterators.get(keyFieldAntimatterStatsEntries.iterator(), 1).getComponentId());
        Assert.assertEquals(mergedValueComponentId,
                Iterators.get(valueFieldAntimatterStatsEntries.iterator(), 1).getComponentId());

        Assert.assertEquals(secondKeyComponentId.getMinTimestamp(), mergedKeyComponentId.getMinTimestamp());
        Assert.assertEquals(secondValueComponentId.getMinTimestamp(), mergedValueComponentId.getMinTimestamp());
        Assert.assertEquals(thirdKeyComponentId.getMaxTimestamp(), mergedKeyComponentId.getMaxTimestamp());
        Assert.assertEquals(thirdValueComponentId.getMaxTimestamp(), mergedValueComponentId.getMaxTimestamp());

        StorageTestUtils.fullMerge(dsLifecycleMgr, primaryLsmBtree, STATS_DATASET);
        StorageTestUtils.fullMerge(dsLifecycleMgr, secondaryLsmBtree, STATS_DATASET);

        Assert.assertEquals(1, keyFieldStatsEntries.size());
        Assert.assertNull(keyFieldStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldStatsEntries.size());
        Assert.assertNull(valueFieldStatsEntries.iterator().next().getSynopsis());
        ComponentStatisticsId fullMergedKeyComponentId = keyFieldStatsEntries.iterator().next().getComponentId();
        ComponentStatisticsId fullMergedValueComponentId = valueFieldStatsEntries.iterator().next().getComponentId();
        Assert.assertEquals(1, keyFieldAntimatterStatsEntries.size());
        Assert.assertNull(keyFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(1, valueFieldAntimatterStatsEntries.size());
        Assert.assertNull(valueFieldAntimatterStatsEntries.iterator().next().getSynopsis());
        Assert.assertEquals(fullMergedKeyComponentId,
                keyFieldAntimatterStatsEntries.iterator().next().getComponentId());
        Assert.assertEquals(fullMergedValueComponentId,
                valueFieldAntimatterStatsEntries.iterator().next().getComponentId());

        Assert.assertEquals(firstKeyComponentId.getMinTimestamp(), fullMergedKeyComponentId.getMinTimestamp());
        Assert.assertEquals(firstValueComponentId.getMinTimestamp(), fullMergedValueComponentId.getMinTimestamp());
        Assert.assertEquals(thirdKeyComponentId.getMaxTimestamp(), fullMergedKeyComponentId.getMaxTimestamp());
        Assert.assertEquals(thirdValueComponentId.getMaxTimestamp(), fullMergedValueComponentId.getMaxTimestamp());
    }
}
