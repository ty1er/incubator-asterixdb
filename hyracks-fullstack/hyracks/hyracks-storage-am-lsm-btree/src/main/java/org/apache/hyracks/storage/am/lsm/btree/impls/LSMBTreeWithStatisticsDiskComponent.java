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

import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.ChainedLSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;
import org.apache.hyracks.storage.common.IIndex;

public class LSMBTreeWithStatisticsDiskComponent implements ILSMDiskComponent {
    private final ILSMDiskComponent wrapperComponent;
    private final IStatisticsFactory statisticsFactory;
    private final IStatisticsManager statisticsManager;

    private ComponentStatistics statistics;

    public LSMBTreeWithStatisticsDiskComponent(ILSMDiskComponent wrapperComponent,
            IStatisticsFactory statisticsFactory, IStatisticsManager statisticsManager) {
        this.wrapperComponent = wrapperComponent;
        this.statisticsFactory = statisticsFactory;
        this.statisticsManager = statisticsManager;
        this.statistics = new ComponentStatistics(-1L, -1L);
    }

    public ComponentStatistics getStatistics() {
        return statistics;
    }

    @Override
    public ChainedLSMDiskComponentBulkLoader createBulkLoader(float fillFactor, boolean verifyInput,
            long numElementsHint, long numAntimatterElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent) throws HyracksDataException {
        ChainedLSMDiskComponentBulkLoader bulkLoaderChain = wrapperComponent.createBulkLoader(fillFactor, verifyInput,
                numElementsHint, numAntimatterElementsHint, checkIfEmptyIndex, withFilter, cleanupEmptyComponent);
        statistics = new ComponentStatistics(numElementsHint, numAntimatterElementsHint);
        // using the fact that cleanupEmptyComponent == true for component bulkload to distinguish it from flush\merge
        bulkLoaderChain.addBulkLoader(new StatisticsBulkLoader(
                statisticsFactory.createStatistics(statistics, cleanupEmptyComponent), statisticsManager, this));
        return bulkLoaderChain;
    }

    @Override
    public void activate(boolean createNewComponent) throws HyracksDataException {
        wrapperComponent.activate(createNewComponent);
        if (!createNewComponent) {
            statistics.readTuplesNum(wrapperComponent.getMetadata());
        }
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean isMutableComponent) throws HyracksDataException {
        return wrapperComponent.threadEnter(opType, isMutableComponent);
    }

    @Override
    public void threadExit(LSMOperationType opType, boolean failedOperation, boolean isMutableComponent)
            throws HyracksDataException {
        wrapperComponent.threadExit(opType, failedOperation, isMutableComponent);
    }

    @Override
    public ComponentState getState() {
        return wrapperComponent.getState();
    }

    @Override
    public LSMComponentType getType() {
        return wrapperComponent.getType();
    }

    @Override
    public DiskComponentMetadata getMetadata() {
        return wrapperComponent.getMetadata();
    }

    @Override
    public ILSMComponentFilter getLSMComponentFilter() {
        return wrapperComponent.getLSMComponentFilter();
    }

    @Override
    public IIndex getIndex() {
        return wrapperComponent.getIndex();
    }

    @Override
    public long getComponentSize() {
        return wrapperComponent.getComponentSize();
    }

    @Override
    public int getFileReferenceCount() {
        return wrapperComponent.getFileReferenceCount();
    }

    @Override
    public ILSMDiskComponentId getComponentId() throws HyracksDataException {
        return wrapperComponent.getComponentId();
    }

    @Override
    public AbstractLSMIndex getLsmIndex() {
        return wrapperComponent.getLsmIndex();
    }

    @Override
    public ITreeIndex getMetadataHolder() {
        return wrapperComponent.getMetadataHolder();
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        return wrapperComponent.getLSMComponentPhysicalFiles();
    }

    @Override
    public void markAsValid(boolean persist) throws HyracksDataException {
        wrapperComponent.markAsValid(persist);
    }

    @Override
    public void deactivateAndDestroy() throws HyracksDataException {
        wrapperComponent.deactivateAndDestroy();
    }

    @Override
    public void destroy() throws HyracksDataException {
        wrapperComponent.destroy();
    }

    @Override
    public void deactivate() throws HyracksDataException {
        wrapperComponent.deactivate();
    }

    @Override
    public void deactivateAndPurge() throws HyracksDataException {
        wrapperComponent.deactivateAndPurge();
    }

    @Override
    public void validate() throws HyracksDataException {
        wrapperComponent.validate();
    }

    @Override
    public IChainedComponentBulkLoader createFilterBulkLoader() throws HyracksDataException {
        return wrapperComponent.createFilterBulkLoader();
    }

    @Override
    public IChainedComponentBulkLoader createIndexBulkLoader(float fillFactor, boolean verifyInput,
            long numElementsHint, boolean checkIfEmptyIndex) throws HyracksDataException {
        return wrapperComponent.createIndexBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
    }
}
