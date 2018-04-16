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
package org.apache.hyracks.storage.am.lsm.btree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractStatisticsFactory;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.DelegatingSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;

public class TestNoAntimatterStatisticsFactory extends AbstractStatisticsFactory {

    public TestNoAntimatterStatisticsFactory(IFieldExtractor[] fieldExtractors) {
        super(fieldExtractors);
    }

    @Override
    public boolean canCollectStats(boolean unorderedTuples) {
        return true;
    }

    @Override
    protected AbstractSynopsisBuilder createSynopsisBuilder(ComponentStatistics componentStatistics,
            boolean isAntimatter, IFieldExtractor fieldExtractor) throws HyracksDataException {
        return new TestSynopsisBuilder(new TestSynopsis(), "", "", "", fieldExtractor.getFieldName(), isAntimatter,
                fieldExtractor, componentStatistics);
    }

    @Override
    public ISynopsisBuilder createStatistics(ComponentStatistics componentStatistics, boolean isBulkload)
            throws HyracksDataException {
        ISynopsisBuilder[] builders = new ISynopsisBuilder[extractors.length];
        for (int i = 0; i < extractors.length; i++) {
            builders[i] = createSynopsisBuilder(componentStatistics, false, extractors[i]);
        }
        if (builders.length == 1) {
            return builders[0];
        } else {
            return new DelegatingSynopsisBuilder(builders);
        }
    }
}