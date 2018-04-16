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
package org.apache.asterix.statistics;

import static org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes.PROPERTIES_STATISTICS;

import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Statistics;
import org.apache.asterix.metadata.utils.MetadataLockUtil;
import org.apache.asterix.statistics.message.ReportFlushComponentStatisticsMessage;
import org.apache.asterix.statistics.message.ReportMergeComponentStatisticsMessage;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.common.ComponentStatisticsId;

public class StatisticsMetadataUtil {
    private final static Logger LOGGER = Logger.getLogger(StatisticsMetadataUtil.class.getName());

    public static void createMergeStatistics(ReportMergeComponentStatisticsMessage mergeMessage,
            IMetadataLockManager metadataLockManager, MetadataProvider mdProvider) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("THREAD [" + Thread.currentThread().getName() + "] received MERGE statistics message for field "
                    + mergeMessage.getDataverse() + "." + mergeMessage.getDataset() + "." + mergeMessage.getIndex()
                    + "." + mergeMessage.getField() + " from node=" + mergeMessage.getNode() + ", partition="
                    + mergeMessage.getPartition() + ", componentId=" + mergeMessage.getComponentId());
        }
        boolean bActiveTxn = false;
        MetadataTransactionContext mdTxnCtx = null;
        try {
            // transactionally update metadata with received statistics
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            mdProvider.setMetadataTxnContext(mdTxnCtx);
            bActiveTxn = true;
            MetadataLockUtil.insertStatisticsBegin(metadataLockManager, mdProvider.getLocks(),
                    PROPERTIES_STATISTICS.getDatasetName(), mergeMessage.getDataverse(), mergeMessage.getDataset(),
                    mergeMessage.getIndex(), mergeMessage.getField(), mergeMessage.getNode(),
                    mergeMessage.getPartition(), mergeMessage.isAntimatter());

            // delete old stats. Even if the new stats are empty we need to invalidate earlier synopses
            for (ComponentStatisticsId mergedComponentId : mergeMessage.getMergeComponentIds()) {
                insertDeleteStats(mdTxnCtx, mergeMessage.getDataverse(), mergeMessage.getDataset(),
                        mergeMessage.getIndex(), mergeMessage.getField(), mergeMessage.getNode(),
                        mergeMessage.getPartition(), mergedComponentId, mergeMessage.isAntimatter(), null, false);
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("THREAD [" + Thread.currentThread().getName() + "]: MSG[" + mergeMessage
                            + "] Deleting old stat with componentId " + mergedComponentId);
                }
            }
            // insert statistics on merged component only if synopsis is not empty, i.e. skip empty statistics
            if (mergeMessage.getSynopsis() != null) {
                insertDeleteStats(mdTxnCtx, mergeMessage.getDataverse(), mergeMessage.getDataset(),
                        mergeMessage.getIndex(), mergeMessage.getField(), mergeMessage.getNode(),
                        mergeMessage.getPartition(), mergeMessage.getComponentId(), mergeMessage.isAntimatter(),
                        mergeMessage.getSynopsis(), true);
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("THREAD [" + Thread.currentThread().getName() + "]: MSG[" + mergeMessage
                            + "] Adding new stat with componentId " + mergeMessage.getComponentId());
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
        } catch (AlgebricksException | ACIDException | RemoteException me) {
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

    public static void createFlushStatistics(ReportFlushComponentStatisticsMessage flushMessage,
            IMetadataLockManager metadataLockManager, MetadataProvider mdProvider) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("THREAD [" + Thread.currentThread().getName() + "] received FLUSH statistics message for field "
                    + flushMessage.getDataverse() + "." + flushMessage.getDataset() + "." + flushMessage.getIndex()
                    + "." + flushMessage.getField() + " from node=" + flushMessage.getNode() + ", partition="
                    + flushMessage.getPartition() + ",componentId=" + flushMessage.getComponentId());
        }
        boolean bActiveTxn = false;
        MetadataTransactionContext mdTxnCtx = null;
        try {
            // transactionally update metadata with received statistics
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            mdProvider.setMetadataTxnContext(mdTxnCtx);
            bActiveTxn = true;
            MetadataLockUtil.insertStatisticsBegin(metadataLockManager, mdProvider.getLocks(),
                    PROPERTIES_STATISTICS.getDatasetName(), flushMessage.getDataverse(), flushMessage.getDataset(),
                    flushMessage.getIndex(), flushMessage.getField(), flushMessage.getNode(),
                    flushMessage.getPartition(), flushMessage.isAntimatter());
            insertDeleteStats(mdTxnCtx, flushMessage.getDataverse(), flushMessage.getDataset(), flushMessage.getIndex(),
                    flushMessage.getField(), flushMessage.getNode(), flushMessage.getPartition(),
                    flushMessage.getComponentId(), flushMessage.isAntimatter(), flushMessage.getSynopsis(), true);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine(
                        "MSG[" + flushMessage + "] Adding new stat with componentId " + flushMessage.getComponentId());
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
        } catch (AlgebricksException | ACIDException | RemoteException me) {
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

    private static void insertDeleteStats(MetadataTransactionContext mdTxnCtx, String dataverseName, String datasetName,
            String indexName, String fieldName, String node, String partition, ComponentStatisticsId componentId,
            boolean isAntimatter, ISynopsis<? extends ISynopsisElement<Long>> synopsis, boolean isInsert)
            throws AlgebricksException {
        if (isInsert) {
            MetadataManager.INSTANCE.addStatistics(mdTxnCtx, new Statistics(dataverseName, datasetName, indexName,
                    fieldName, node, partition, componentId, false, isAntimatter, synopsis));
        } else {
            MetadataManager.INSTANCE.dropStatistics(mdTxnCtx, dataverseName, datasetName, indexName, fieldName, node,
                    partition, componentId, isAntimatter);
        }
    }
}
