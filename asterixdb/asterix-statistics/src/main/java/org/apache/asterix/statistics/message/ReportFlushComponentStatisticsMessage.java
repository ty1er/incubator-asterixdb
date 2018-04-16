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
package org.apache.asterix.statistics.message;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.statistics.StatisticsMetadataUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.common.ComponentStatisticsId;

public class ReportFlushComponentStatisticsMessage implements ICcAddressedMessage {
    private static final long serialVersionUID = 1L;

    private ISynopsis<? extends ISynopsisElement<Long>> synopsis;
    private String dataverse;
    private String dataset;
    private String index;
    private String field;
    private String node;
    private String partition;
    private boolean isAntimatter;
    private ComponentStatisticsId componentId;

    public ReportFlushComponentStatisticsMessage(ISynopsis<? extends ISynopsisElement<Long>> synopsis, String dataverse,
            String dataset, String index, String field, String node, String partition,
            ComponentStatisticsId componentId, boolean isAntimatter) {
        this.synopsis = synopsis;
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.index = index;
        this.field = field;
        this.node = node;
        this.partition = partition;
        this.componentId = componentId;
        this.isAntimatter = isAntimatter;
    }

    public ISynopsis<? extends ISynopsisElement<Long>> getSynopsis() {
        return synopsis;
    }

    public String getDataverse() {
        return dataverse;
    }

    public String getDataset() {
        return dataset;
    }

    public String getIndex() {
        return index;
    }

    public String getField() {
        return field;
    }

    public String getNode() {
        return node;
    }

    public String getPartition() {
        return partition;
    }

    public boolean isAntimatter() {
        return isAntimatter;
    }

    public ComponentStatisticsId getComponentId() {
        return componentId;
    }

    @Override
    public String toString() {
        return ReportFlushComponentStatisticsMessage.class.getSimpleName();
    }

    @Override
    public void handle(ICcApplicationContext cs) throws HyracksDataException, InterruptedException {
        MetadataProvider mdProvider = new MetadataProvider(cs, null);
        StatisticsMetadataUtil.createFlushStatistics(this, cs.getMetadataLockManager(), mdProvider);
    }
}
