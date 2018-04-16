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
package org.apache.hyracks.storage.am.statistics.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;

public abstract class AbstractSynopsisBuilder<T extends ISynopsis<? extends ISynopsisElement<V>>, V>
        implements ISynopsisBuilder {

    protected boolean isEmpty = true;

    protected final T synopsis;
    private final String dataverse;
    private final String dataset;
    private final String index;
    private final String field;
    protected final boolean isAntimatter;
    private final IFieldExtractor<V> fieldExtractor;
    private final ComponentStatistics componentStatistics;
    private long numTuples = 0L;

    public AbstractSynopsisBuilder(T synopsis, String dataverse, String dataset, String index, String field,
            boolean isAntimatter, IFieldExtractor<V> fieldExtractor, ComponentStatistics componentStatistics) {
        this.synopsis = synopsis;
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.index = index;
        this.field = field;
        this.isAntimatter = isAntimatter;
        this.fieldExtractor = fieldExtractor;
        this.componentStatistics = componentStatistics;
    }

    @Override
    public void gatherComponentStatistics(IStatisticsManager statisticsManager, ILSMDiskComponent component)
            throws HyracksDataException {
        // Skip sending statistics about empty synopses
        if (!isEmpty) {
            statisticsManager.addStatistics(synopsis, dataverse, dataset, index, field, isAntimatter, component);
        } else {
            statisticsManager.addStatistics(null, dataverse, dataset, index, field, isAntimatter, component);
        }
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        numTuples++;
        V value = fieldExtractor.extractFieldValue(tuple);
        addValue(value);
        isEmpty = false;
    }

    @Override
    public void end() throws HyracksDataException {
        if (componentStatistics != null) {
            if (isAntimatter) {
                componentStatistics.resetAntimatterTuples(numTuples);
            } else {
                componentStatistics.resetTuples(numTuples);
            }
        }
        if (!isEmpty) {
            finishSynopsisBuild();
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        //Noop
    }

    public abstract void addValue(V value);

    public abstract void finishSynopsisBuild() throws HyracksDataException;
}
