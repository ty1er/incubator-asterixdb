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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;

public class StatisticsBulkLoader implements IChainedComponentBulkLoader {

    protected final ISynopsisBuilder statisticsBuilder;
    private final LSMBTreeWithStatisticsDiskComponent component;
    protected final IStatisticsManager statisticsManager;

    public StatisticsBulkLoader(ISynopsisBuilder statisticsBuilder, IStatisticsManager statisticsManager,
            LSMBTreeWithStatisticsDiskComponent component) {
        this.statisticsBuilder = statisticsBuilder;
        this.statisticsManager = statisticsManager;
        this.component = component;
    }

    @Override
    public ITupleReference add(ITupleReference tuple) throws HyracksDataException {
        statisticsBuilder.add(tuple);
        return tuple;
    }

    @Override
    public ITupleReference delete(ITupleReference tuple) throws HyracksDataException {
        statisticsBuilder.add(tuple);
        return tuple;
    }

    @Override
    public void end() throws HyracksDataException {
        statisticsBuilder.end();
        statisticsBuilder.gatherComponentStatistics(statisticsManager, component);
        component.getStatistics().writeTuplesNum(component.getMetadata());
    }

    @Override
    public void abort() throws HyracksDataException {
        //Noop
    }

    @Override
    public void cleanupArtifacts() throws HyracksDataException {
        //Noop
    }
}
