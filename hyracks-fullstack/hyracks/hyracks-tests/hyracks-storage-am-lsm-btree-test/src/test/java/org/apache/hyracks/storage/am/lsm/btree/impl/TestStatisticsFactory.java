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

import java.util.List;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.common.AbstractStatisticsFactory;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;

public class TestStatisticsFactory extends AbstractStatisticsFactory {

    private final int numKeys;

    public TestStatisticsFactory(List<String> fields, List<ITypeTraits> fieldTypeTraits,
            List<IFieldExtractor> fieldValueExtractors, int numKeys) {
        super(fields, fieldTypeTraits, fieldValueExtractors);
        this.numKeys = numKeys;
    }

    @Override
    public boolean canCollectStats(boolean unorderedTuples) {
        return true;
    }

    @Override
    protected AbstractSynopsisBuilder createSynopsisBuilder(ComponentStatistics componentStatistics,
            boolean isAntimatter, String fieldName, ITypeTraits fieldTraits, IFieldExtractor fieldExtractor)
            throws HyracksDataException {
        return new TestSynopsisBuilder(new TestSynopsis(), "", "", "", fieldName, isAntimatter, fieldExtractor,
                componentStatistics);
    }
}
