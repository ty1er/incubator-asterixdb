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

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_STATISTICS;

import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Statistics;
import org.apache.asterix.metadata.utils.MetadataLockUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.common.ComponentStatisticsId;

public class ReportFlushComponentStatisticsMessage implements ICcAddressedMessage {
    private static final long serialVersionUID = 1L;
    private final static Logger LOGGER = Logger.getLogger(ReportFlushComponentStatisticsMessage.class.getName());

    protected ISynopsis<? extends ISynopsisElement> synopsis;
    protected String dataverse;
    protected String dataset;
    protected String index;
    protected String node;
    protected String partition;
    protected boolean isAntimatter;
    protected ComponentStatisticsId componentId;

    public ReportFlushComponentStatisticsMessage(ISynopsis<? extends ISynopsisElement> synopsis, String dataverse,
            String dataset, String index, String node, String partition, ComponentStatisticsId componentId,
            boolean isAntimatter) {
        this.synopsis = synopsis;
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.index = index;
        this.node = node;
        this.partition = partition;
        this.componentId = componentId;
        this.isAntimatter = isAntimatter;
    }

    @Override public String toString() {
        return ReportFlushComponentStatisticsMessage.class.getSimpleName();
    }

    @Override
    public void handle(ICcApplicationContext cs) throws HyracksDataException, InterruptedException {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("THREAD [" + Thread.currentThread().getName() + "]: MSG[" + this + "] message for idx ["
                    + dataverse + "." + dataset + "." + index + "] received from the node=" + node + ", partition="
                    + partition + ",componentId=" + componentId);
        }
        boolean bActiveTxn = false;
        MetadataProvider mdProvider = new MetadataProvider(cs, null);
        MetadataTransactionContext mdTxnCtx = null;
        try {
            // transactionally update metadata with received statistics
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            mdProvider.setMetadataTxnContext(mdTxnCtx);
            bActiveTxn = true;
            MetadataLockUtil.insertStatisticsBegin(cs.getMetadataLockManager(), mdProvider.getLocks(),
                    PROPERTIES_STATISTICS.getDatasetName(), dataverse, dataset, index, node, partition, isAntimatter);
            insertDeleteStats(mdTxnCtx, dataverse, dataset, index, node, partition, componentId, isAntimatter, synopsis,
                    true);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("MSG[" + this + "] Adding new stat with componentId " + componentId);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
        } catch (MetadataException | AsterixException | ACIDException | RemoteException me) {
            if (bActiveTxn) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                } catch (ACIDException | RemoteException e) {
                    throw new HyracksDataException("Failed to abort metadata transaction", e);
                }
            }
            throw HyracksDataException.create(me);
        } finally {
            mdProvider.getLocks().unlock();
        }
    }

    protected void insertDeleteStats(MetadataTransactionContext mdTxnCtx, String dataverseName, String datasetName,
            String indexName, String node, String partition, ComponentStatisticsId componentId, boolean isAntimatter,
            ISynopsis<? extends ISynopsisElement> synopsis, boolean isInsert) throws MetadataException {
        if (isInsert) {
            MetadataManager.INSTANCE.addStatistics(mdTxnCtx, new Statistics(dataverseName, datasetName, indexName, node,
                    partition, componentId, false, isAntimatter, synopsis));
        } else {
            MetadataManager.INSTANCE.dropStatistics(mdTxnCtx, dataverseName, datasetName, indexName, node, partition,
                    componentId, isAntimatter);
        }
    }
}
