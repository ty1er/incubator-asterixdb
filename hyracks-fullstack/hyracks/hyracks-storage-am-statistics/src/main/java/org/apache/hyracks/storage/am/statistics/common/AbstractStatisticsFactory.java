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

import java.io.Serializable;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;

public abstract class AbstractStatisticsFactory implements IStatisticsFactory, Serializable {

    protected final IFieldExtractor[] extractors;

    protected AbstractStatisticsFactory(IFieldExtractor[] extractors) {
        this.extractors = extractors;
    }

    @Override
    public ISynopsisBuilder createStatistics(ComponentStatistics componentStatistics, boolean isBulkload)
            throws HyracksDataException {
        ISynopsisBuilder[] builders = new ISynopsisBuilder[extractors.length];
        for (int i = 0; i < extractors.length; i++) {
            AbstractSynopsisBuilder synopsisBuilder = createSynopsisBuilder(componentStatistics, false, extractors[i]);
            if (isBulkload) {
                builders[i] = synopsisBuilder;
            } else {
                AbstractSynopsisBuilder antimatterSynopsisBuilder =
                        createSynopsisBuilder(componentStatistics, true, extractors[i]);
                builders[i] = new CombinedSynopsisBuilder(synopsisBuilder, antimatterSynopsisBuilder);
            }
        }
        if (builders.length == 1) {
            return builders[0];
        } else {
            return new DelegatingSynopsisBuilder(builders);
        }
    }

    protected abstract AbstractSynopsisBuilder createSynopsisBuilder(ComponentStatistics componentStatistics,
            boolean isAntimatter, IFieldExtractor fieldExtractor) throws HyracksDataException;
}
