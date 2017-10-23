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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.ChainedLSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;

public class LSMBTreeWithStatisticsDiskComponent extends LSMBTreeDiskComponent {
    private final IStatisticsFactory statisticsFactory;
    private final IStatisticsManager statisticsManager;

    private ComponentStatistics statistics;

    public LSMBTreeWithStatisticsDiskComponent(AbstractLSMIndex lsmIndex, BTree btree, ILSMComponentFilter filter,
            IStatisticsFactory statisticsFactory, IStatisticsManager statisticsManager) {
        super(lsmIndex, btree, filter);
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
        ChainedLSMDiskComponentBulkLoader bulkLoaderChain = super.createBulkLoader(fillFactor, verifyInput,
                numElementsHint, numAntimatterElementsHint, checkIfEmptyIndex, withFilter, cleanupEmptyComponent);
        statistics = new ComponentStatistics(numElementsHint, numAntimatterElementsHint);
        // using the fact that cleanupEmptyComponent == true for component bulkload to distinguish it from flush\merge
        bulkLoaderChain.addBulkLoader(new StatisticsBulkLoader(
                statisticsFactory.createStatistics(statistics, cleanupEmptyComponent), statisticsManager, this));
        return bulkLoaderChain;
    }

    @Override
    public void activate(boolean createNewComponent) throws HyracksDataException {
        super.activate(createNewComponent);
        if (!createNewComponent) {
            statistics.readTuplesNum(getMetadata());
        }
    }
}
