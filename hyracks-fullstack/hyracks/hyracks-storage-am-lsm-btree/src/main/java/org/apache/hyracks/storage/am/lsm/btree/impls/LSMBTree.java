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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBloomFilterDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.am.statistics.common.StatisticsMessageIOOperationCallback;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class LSMBTree extends AbstractLSMIndex implements ITreeIndex {

    private static final ICursorFactory cursorFactory = opCtx -> new LSMBTreeSearchCursor(opCtx);
    // Common for in-memory and on-disk components.
    protected final ITreeIndexFrameFactory insertLeafFrameFactory;
    protected final ITreeIndexFrameFactory deleteLeafFrameFactory;
    protected final IBinaryComparatorFactory[] cmpFactories;
    private final boolean updateAware;

    private final boolean needKeyDupCheck;

    // Primary LSMBTree has a Bloomfilter, but Secondary one doesn't have.
    private final boolean hasBloomFilter;

    private boolean hasStatistics;
    private IStatisticsManager statisticsManager;

    public LSMBTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, boolean needKeyDupCheck, int[] btreeFields, int[] filterFields,
            boolean durable, boolean updateAware, ITracer tracer, IStatisticsManager statisticsManager)
            throws HyracksDataException {
        super(ioManager, virtualBufferCaches, diskBufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy,
                opTracker, ioScheduler, ioOpCallback, componentFactory, bulkLoadComponentFactory, filterFrameFactory,
                filterManager, filterFields, durable, filterHelper, btreeFields, tracer);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.updateAware = updateAware;
        this.statisticsManager = statisticsManager;
        this.hasStatistics = statisticsManager != null;
        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            LSMBTreeMemoryComponent mutableComponent = new LSMBTreeMemoryComponent(
                    new BTree(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache), interiorFrameFactory,
                            insertLeafFrameFactory, cmpFactories, fieldCount,
                            ioManager.resolveAbsolutePath(fileManager.getBaseDir() + "_virtual_" + i)),
                    virtualBufferCache, i == 0 ? true : false,
                    filterHelper == null ? null : filterHelper.createFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }
        this.needKeyDupCheck = needKeyDupCheck;
        this.hasBloomFilter = needKeyDupCheck;
    }

    // Without memory components
    public LSMBTree(IIOManager ioManager, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, IBufferCache bufferCache, ILSMIndexFileManager fileManager,
            ILSMDiskComponentFactory componentFactory, ILSMDiskComponentFactory bulkLoadComponentFactory,
            double bloomFilterFalsePositiveRate, IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback,
            boolean needKeyDupCheck, boolean durable) {
        super(ioManager, bufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler,
                ioOpCallback, componentFactory, bulkLoadComponentFactory, durable);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.needKeyDupCheck = needKeyDupCheck;
        this.hasBloomFilter = true;
        this.updateAware = false;
    }

    @Override
    public boolean isPrimaryIndex() {
        return needKeyDupCheck;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
            ctx.getCurrentMutableBTreeAccessor().getOpContext().resetNonIndexFieldsTuple(tuple);
        } else {
            indexTuple = tuple;
        }

        switch (ctx.getOperation()) {
            case PHYSICALDELETE:
                ctx.getCurrentMutableBTreeAccessor().delete(indexTuple);
                break;
            case INSERT:
                insert(indexTuple, ctx);
                break;
            default:
                ctx.getCurrentMutableBTreeAccessor().upsert(indexTuple);
                break;
        }
        updateFilter(ctx, tuple);
    }

    private boolean insert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException {
        LSMBTreePointSearchCursor searchCursor = ctx.getInsertSearchCursor();
        IIndexCursor memCursor = ctx.getMemCursor();
        RangePredicate predicate = (RangePredicate) ctx.getSearchPredicate();
        predicate.setHighKey(tuple);
        predicate.setLowKey(tuple);
        if (needKeyDupCheck) {
            // first check the inmemory component
            ctx.getCurrentMutableBTreeAccessor().search(memCursor, predicate);
            try {
                if (memCursor.hasNext()) {
                    memCursor.next();
                    LSMBTreeTupleReference lsmbtreeTuple = (LSMBTreeTupleReference) memCursor.getTuple();
                    if (!lsmbtreeTuple.isAntimatter()) {
                        throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
                    } else {
                        memCursor.close();
                        ctx.getCurrentMutableBTreeAccessor().upsertIfConditionElseInsert(tuple,
                                AntimatterAwareTupleAcceptor.INSTANCE);
                        return true;
                    }
                }
            } finally {
                memCursor.close();
            }

            // TODO: Can we just remove the above code that search the mutable
            // component and do it together with the search call below? i.e. instead
            // of passing false to the lsmHarness.search(), we pass true to include
            // the mutable component?
            // the key was not in the inmemory component, so check the disk
            // components

            // This is a hack to avoid searching the current active mutable component twice. It is critical to add it back once the search is over.
            ILSMComponent firstComponent = ctx.getComponentHolder().remove(0);
            search(ctx, searchCursor, predicate);
            try {
                if (searchCursor.hasNext()) {
                    throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
                }
            } finally {
                searchCursor.close();
                // Add the current active mutable component back
                ctx.getComponentHolder().add(0, firstComponent);
            }
        }
        ctx.getCurrentMutableBTreeAccessor().upsertIfConditionElseInsert(tuple, AntimatterAwareTupleAcceptor.INSTANCE);
        return true;
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        ctx.getSearchInitialState().reset(pred, operationalComponents);
        cursor.open(ctx.getSearchInitialState(), pred);
    }

    @Override
    public void scanDiskComponents(ILSMIndexOperationContext ictx, IIndexCursor cursor) throws HyracksDataException {
        if (!isPrimaryIndex()) {
            throw HyracksDataException.create(ErrorCode.DISK_COMPONENT_SCAN_NOT_ALLOWED_FOR_SECONDARY_INDEX);
        }
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        MultiComparator comp = MultiComparator.create(getComparatorFactories());
        ISearchPredicate pred = new RangePredicate(null, null, true, true, comp, comp);
        ctx.getSearchInitialState().reset(pred, operationalComponents);
        ((LSMBTreeSearchCursor) cursor).scan(ctx.getSearchInitialState(), pred);
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeFlushOperation flushOp = (LSMBTreeFlushOperation) operation;
        LSMBTreeMemoryComponent flushingComponent = (LSMBTreeMemoryComponent) flushOp.getFlushingComponent();
        IIndexAccessor accessor = flushingComponent.getIndex().createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);

        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        long numElements = 0L;
        long numAntimatterElements = 0L;
        if (hasBloomFilter || hasStatistics) {
            //count elements in btree for creating Bloomfilter or statistics
            IIndexCursor countingCursor = accessor.createSearchCursor(false);
            accessor.search(countingCursor, nullPred);
            try {
                while (countingCursor.hasNext()) {
                    countingCursor.next();
                    LSMBTreeTupleReference tuple = (LSMBTreeTupleReference) countingCursor.getTuple();
                    if (tuple.isUpdated()) {
                        continue;
                    }
                    if (tuple.isAntimatter()) {
                        numAntimatterElements++;
                    } else {
                        numElements++;
                    }
                }
            } finally {
                countingCursor.close();
            }
        }

        ILSMDiskComponent component =
                createDiskComponent(componentFactory, flushOp.getTarget(), null, flushOp.getBloomFilterTarget(), true);

        ILSMDiskComponentBulkLoader componentBulkLoader =
                component.createBulkLoader(1.0f, false, numElements, numAntimatterElements, false, false, false);

        IIndexCursor scanCursor = accessor.createSearchCursor(false);
        accessor.search(scanCursor, nullPred);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                LSMBTreeTupleReference tupleRef = (LSMBTreeTupleReference) scanCursor.getTuple();
                // we can safely throw away updated tuples in secondary BTree components, because they correspond to
                // deleted tuples
                if (updateAware && tupleRef.isUpdated()) {
                    continue;
                }
                componentBulkLoader.add(tupleRef);
            }
        } finally {
            scanCursor.close();
        }

        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            getFilterManager().updateFilter(component.getLSMComponentFilter(), filterTuples);
            getFilterManager().writeFilter(component.getLSMComponentFilter(), component.getMetadataHolder());
        }
        // Write metadata from memory component to disk
        // Q. what about the merge operation? how do we resolve conflicts
        // A. Through providing an appropriate ILSMIOOperationCallback
        // Must not reset the metadata before the flush is completed
        // Use the copy of the metadata in the opContext
        // TODO This code should be in the callback and not in the index
        flushingComponent.getMetadata().copy(component.getMetadata());

        componentBulkLoader.end();

        return component;
    }

    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        search(mergeOp.getAccessor().getOpContext(), cursor, rangePred);
        List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
        long numElements = 0L;
        long numAntimatterElements = 0L;
        if (hasBloomFilter) {
            //count elements in btree for creating Bloomfilter
            for (int i = 0; i < mergedComponents.size(); ++i) {
                numElements += ((AbstractLSMWithBloomFilterDiskComponent) mergedComponents.get(i)).getBloomFilter()
                        .getNumElements();
            }
        }
        if (hasStatistics) {
            numElements = 0L;
            for (ILSMComponent component : mergedComponents) {
                numElements += ((AbstractLSMDiskComponent) component).getStatistics().getNumTuples();
                numAntimatterElements +=
                        ((AbstractLSMDiskComponent) component).getStatistics().getNumAntimatterTuples();
            }
        }
        ILSMDiskComponent mergedComponent =
                createDiskComponent(componentFactory, mergeOp.getTarget(), null, mergeOp.getBloomFilterTarget(), true);

        ILSMDiskComponentBulkLoader componentBulkLoader =
                mergedComponent.createBulkLoader(1.0f, false, numElements, numAntimatterElements, false, false, false);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                LSMBTreeTupleReference frameTuple = (LSMBTreeTupleReference) cursor.getTuple();
                if (updateAware && frameTuple.isUpdated()) {
                    continue;
                }
                componentBulkLoader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }
        if (mergedComponent.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
            }
            getFilterManager().updateFilter(mergedComponent.getLSMComponentFilter(), filterTuples);
            getFilterManager().writeFilter(mergedComponent.getLSMComponentFilter(),
                    mergedComponent.getMetadataHolder());
        }
        componentBulkLoader.end();
        return mergedComponent;
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        ILSMIOOperationCallback callback = ioOpCallback;
        if (hasStatistics) {
            callback = new StatisticsMessageIOOperationCallback(ioOpCallback, statisticsManager);
        }
        return new LSMIndexDiskComponentBulkLoader(this, callback, fillLevel, verifyInput, numElementsHint);
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback) {
        ILSMIndexAccessor accessor = createAccessor(opCtx);
        if (hasStatistics) {
            callback = new StatisticsMessageIOOperationCallback(callback, statisticsManager);
        }
        return new LSMBTreeFlushOperation(accessor, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir().getAbsolutePath());
    }

    @Override
    public LSMBTreeOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        int numBloomFilterKeyFields = hasBloomFilter
                ? ((LSMBTreeWithBloomFilterDiskComponentFactory) componentFactory).getBloomFilterKeyFields().length
                : 0;
        return new LSMBTreeOpContext(memoryComponents, insertLeafFrameFactory, deleteLeafFrameFactory,
                modificationCallback, searchCallback, numBloomFilterKeyFields, getTreeFields(), getFilterFields(),
                getLsmHarness(), getFilterCmpFactories());
    }

    @Override
    public ILSMIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return createAccessor(createOpContext(modificationCallback, searchCallback));
    }

    public ILSMIndexAccessor createAccessor(AbstractLSMIndexOperationContext opCtx) {
        return new LSMTreeIndexAccessor(getLsmHarness(), opCtx, cursorFactory);
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getInteriorFrameFactory();
    }

    @Override
    public int getFieldCount() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFieldCount();
    }

    @Override
    public int getFileId() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFileId();
    }

    @Override
    public IPageManager getPageManager() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getPageManager();
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getLeafFrameFactory();
    }

    @Override
    public int getRootPageId() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getRootPageId();
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException {
        BTree firstBTree = (BTree) firstComponent.getIndex();
        BTree lastBTree = (BTree) lastComponent.getIndex();
        FileReference firstFile = firstBTree.getFileReference();
        FileReference lastFile = lastBTree.getFileReference();
        return fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) {
        boolean returnDeletedTuples = false;
        ILSMIndexAccessor accessor = createAccessor(opCtx);
        List<ILSMComponent> mergingComponents = opCtx.getComponentHolder();
        if (mergingComponents.get(mergingComponents.size() - 1) != diskComponents.get(diskComponents.size() - 1)) {
            returnDeletedTuples = true;
        }
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor(opCtx, returnDeletedTuples);
        if (hasStatistics) {
            callback = new StatisticsMessageIOOperationCallback(callback, statisticsManager);
        }
        return new LSMBTreeMergeOperation(accessor, cursor, mergeFileRefs.getInsertIndexFileReference(),
                mergeFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir().getAbsolutePath());
    }
}
