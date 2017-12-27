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
package org.apache.asterix.metadata.utils;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.LockList;

public class MetadataLockUtil {

    private MetadataLockUtil() {
    }

    public static void createDatasetBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String itemTypeDataverseName, String itemTypeFullyQualifiedName, String metaItemTypeDataverseName,
            String metaItemTypeFullyQualifiedName, String nodeGroupName, String compactionPolicyName,
            String datasetFullyQualifiedName, boolean isDefaultCompactionPolicy) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        if (!dataverseName.equals(itemTypeDataverseName)) {
            lockMgr.acquireDataverseReadLock(locks, itemTypeDataverseName);
        }
        if (metaItemTypeDataverseName != null && !metaItemTypeDataverseName.equals(dataverseName)
                && !metaItemTypeDataverseName.equals(itemTypeDataverseName)) {
            lockMgr.acquireDataverseReadLock(locks, metaItemTypeDataverseName);
        }
        lockMgr.acquireDataTypeReadLock(locks, itemTypeFullyQualifiedName);
        if (metaItemTypeFullyQualifiedName != null
                && !metaItemTypeFullyQualifiedName.equals(itemTypeFullyQualifiedName)) {
            lockMgr.acquireDataTypeReadLock(locks, metaItemTypeFullyQualifiedName);
        }
        if (nodeGroupName != null) {
            lockMgr.acquireNodeGroupReadLock(locks, nodeGroupName);
        }
        if (!isDefaultCompactionPolicy) {
            lockMgr.acquireMergePolicyReadLock(locks, compactionPolicyName);
        }
        lockMgr.acquireDatasetWriteLock(locks, datasetFullyQualifiedName);
    }

    public static void createIndexBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String datasetFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetCreateIndexLock(locks, datasetFullyQualifiedName);
    }

    public static void dropIndexBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String datasetFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetWriteLock(locks, datasetFullyQualifiedName);
    }

    public static void createTypeBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String itemTypeFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDataTypeWriteLock(locks, itemTypeFullyQualifiedName);
    }

    public static void dropDatasetBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String datasetFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetWriteLock(locks, datasetFullyQualifiedName);
    }

    public static void dropTypeBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String dataTypeFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDataTypeWriteLock(locks, dataTypeFullyQualifiedName);
    }

    public static void functionStatementBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String functionFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireFunctionWriteLock(locks, functionFullyQualifiedName);
    }

    public static void modifyDatasetBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String datasetFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetModifyLock(locks, datasetFullyQualifiedName);
    }

    public static void insertDeleteUpsertBegin(IMetadataLockManager lockMgr, LockList locks,
            String datasetFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks,
                MetadataUtil.getDataverseFromFullyQualifiedName(datasetFullyQualifiedName));
        lockMgr.acquireDatasetModifyLock(locks, datasetFullyQualifiedName);
    }

    public static void dropFeedBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String feedFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, feedFullyQualifiedName);
    }

    public static void dropFeedPolicyBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String policyName) throws AsterixException {
        lockMgr.acquireActiveEntityWriteLock(locks, policyName);
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
    }

    public static void startFeedBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String feedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, feedName);
    }

    public static void stopFeedBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String feedName) throws AsterixException {
        // TODO: dataset lock?
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, feedName);
    }

    public static void createFeedBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String feedFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, feedFullyQualifiedName);
    }

    public static void connectFeedBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String datasetFullyQualifiedName, String feedFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, feedFullyQualifiedName);
        lockMgr.acquireDatasetReadLock(locks, datasetFullyQualifiedName);
    }

    public static void createFeedPolicyBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String policyName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireFeedPolicyWriteLock(locks, policyName);
    }

    public static void disconnectFeedBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String datasetFullyQualifiedName, String feedFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, feedFullyQualifiedName);
        lockMgr.acquireDatasetReadLock(locks, datasetFullyQualifiedName);
    }

    public static void compactBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String datasetFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetReadLock(locks, datasetFullyQualifiedName);
    }

    public static void refreshDatasetBegin(IMetadataLockManager lockMgr, LockList locks, String dataverseName,
            String datasetFullyQualifiedName) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetExclusiveModificationLock(locks, datasetFullyQualifiedName);
    }

    public static void insertStatisticsBegin(IMetadataLockManager lockMgr, LockList locks, String statisticsDataset,
            String dataverseName, String datasetName, String indexName, String nodeName, String partitionId,
            boolean isAntimatter) throws AsterixException {
        lockMgr.acquireDataverseReadLock(locks, statisticsDataset);
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireStatisticsWriteLock(locks, dataverseName, datasetName, indexName, nodeName, partitionId,
                isAntimatter);
    }

}
