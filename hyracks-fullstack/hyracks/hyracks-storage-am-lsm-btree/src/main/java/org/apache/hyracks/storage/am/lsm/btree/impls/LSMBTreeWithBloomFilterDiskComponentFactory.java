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
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;

public class LSMBTreeWithBloomFilterDiskComponentFactory implements ILSMDiskComponentFactory {
    protected final TreeIndexFactory<BTree> btreeFactory;
    protected final IComponentFilterHelper filterHelper;
    protected final BloomFilterFactory bloomFilterFactory;
    private final IStatisticsFactory statisticsFactory;
    private final IStatisticsManager statisticsManager;

    public LSMBTreeWithBloomFilterDiskComponentFactory(TreeIndexFactory<BTree> btreeFactory,
            BloomFilterFactory bloomFilterFactory, IComponentFilterHelper filterHelper,
            IStatisticsFactory statisticsFactory, IStatisticsManager statisticsManager) {
        this.btreeFactory = btreeFactory;
        this.filterHelper = filterHelper;
        this.bloomFilterFactory = bloomFilterFactory;
        this.statisticsFactory = statisticsFactory;
        this.statisticsManager = statisticsManager;
    }

    @Override
    public ILSMDiskComponent createComponent(AbstractLSMIndex lsmIndex,
            LSMComponentFileReferences cfr) throws HyracksDataException {
        LSMBTreeWithBloomFilterDiskComponent btreeComponent = new LSMBTreeWithBloomFilterDiskComponent(lsmIndex,
                btreeFactory.createIndexInstance(cfr.getInsertIndexFileReference()),
                bloomFilterFactory.createBloomFiltertInstance(cfr.getBloomFilterFileReference()),
                filterHelper == null ? null : filterHelper.createFilter());
        if (statisticsManager != null && statisticsFactory != null) {
            return new LSMBTreeWithStatisticsDiskComponent(btreeComponent, statisticsFactory, statisticsManager);
        }
        return btreeComponent;
    }

    public int[] getBloomFilterKeyFields() {
        return bloomFilterFactory == null ? null : bloomFilterFactory.getBloomFilterKeyFields();
    }
}
