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
package org.apache.asterix.metadata.lock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.IMetadataLock;
import org.apache.asterix.common.metadata.LockList;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class MetadataLockManager implements IMetadataLockManager {

    private static final Function<String, MetadataLock> LOCK_FUNCTION = MetadataLock::new;
    private static final Function<String, DatasetLock> DATASET_LOCK_FUNCTION = DatasetLock::new;

    private final ConcurrentHashMap<String, IMetadataLock> mdlocks;

    private static final String DATAVERSE_PREFIX = "Dataverse:";
    private static final String DATASET_PREFIX = "Dataset:";
    private static final String FUNCTION_PREFIX = "Function:";
    private static final String NODE_GROUP_PREFIX = "NodeGroup:";
    private static final String ACTIVE_PREFIX = "Active:";
    private static final String FEED_POLICY_PREFIX = "FeedPolicy:";
    private static final String MERGE_POLICY_PREFIX = "MergePolicy:";
    private static final String DATATYPE_PREFIX = "DataType:";
    private static final String EXTENSION_PREFIX = "Extension:";
    private static final String STATISTICS_PREFIX = "Statistics:";

    private static final String DELIMITER = ":";

    public MetadataLockManager() {
        mdlocks = new ConcurrentHashMap<>();
    }

    @Override
    public void acquireDataverseReadLock(LockList locks, String dataverseName) throws AsterixException {
        String key = DATAVERSE_PREFIX + dataverseName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDataverseWriteLock(LockList locks, String dataverseName) throws AsterixException {
        String key = DATAVERSE_PREFIX + dataverseName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDatasetReadLock(LockList locks, String datasetName) throws AsterixException {
        String key = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDatasetWriteLock(LockList locks, String datasetName) throws AsterixException {
        String key = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDatasetModifyLock(LockList locks, String datasetName) throws AsterixException {
        String key = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.MODIFY, lock);
    }

    @Override
    public void acquireDatasetCreateIndexLock(LockList locks, String datasetName) throws AsterixException {
        String dsKey = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(dsKey, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.INDEX_BUILD, lock);
    }

    @Override
    public void acquireDatasetExclusiveModificationLock(LockList locks, String datasetName) throws AsterixException {
        String key = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.EXCLUSIVE_MODIFY, lock);
    }

    @Override
    public void acquireFunctionReadLock(LockList locks, String functionName) throws AsterixException {
        String key = FUNCTION_PREFIX + functionName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireFunctionWriteLock(LockList locks, String functionName) throws AsterixException {
        String key = FUNCTION_PREFIX + functionName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireNodeGroupReadLock(LockList locks, String nodeGroupName) throws AsterixException {
        String key = NODE_GROUP_PREFIX + nodeGroupName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireNodeGroupWriteLock(LockList locks, String nodeGroupName) throws AsterixException {
        String key = NODE_GROUP_PREFIX + nodeGroupName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireActiveEntityReadLock(LockList locks, String entityName) throws AsterixException {
        String key = ACTIVE_PREFIX + entityName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireActiveEntityWriteLock(LockList locks, String entityName) throws AsterixException {
        String key = ACTIVE_PREFIX + entityName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireFeedPolicyWriteLock(LockList locks, String feedPolicyName) throws AsterixException {
        String key = FEED_POLICY_PREFIX + feedPolicyName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireFeedPolicyReadLock(LockList locks, String feedPolicyName) throws AsterixException {
        String key = FEED_POLICY_PREFIX + feedPolicyName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireMergePolicyReadLock(LockList locks, String mergePolicyName) throws AsterixException {
        String key = MERGE_POLICY_PREFIX + mergePolicyName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireMergePolicyWriteLock(LockList locks, String mergePolicyName) throws AsterixException {
        String key = MERGE_POLICY_PREFIX + mergePolicyName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDataTypeReadLock(LockList locks, String datatypeName) throws AsterixException {
        String key = DATATYPE_PREFIX + datatypeName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDataTypeWriteLock(LockList locks, String datatypeName) throws AsterixException {
        String key = DATATYPE_PREFIX + datatypeName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireExtensionReadLock(LockList locks, String extension, String entityName) throws AsterixException {
        String key = EXTENSION_PREFIX + extension + entityName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireExtensionWriteLock(LockList locks, String extension, String entityName) throws AsterixException {
        String key = EXTENSION_PREFIX + extension + entityName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void upgradeDatasetLockToWrite(LockList locks, String fullyQualifiedName) throws AlgebricksException {
        String key = DATASET_PREFIX + fullyQualifiedName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.upgrade(IMetadataLock.Mode.UPGRADED_WRITE, lock);
    }

    @Override
    public void downgradeDatasetLockToExclusiveModify(LockList locks, String fullyQualifiedName)
            throws AlgebricksException {
        String key = DATASET_PREFIX + fullyQualifiedName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.downgrade(IMetadataLock.Mode.EXCLUSIVE_MODIFY, lock);
    }

    @Override
    public void acquireStatisticsWriteLock(LockList locks, String dataverse, String dataset, String indexName,
            String fieldName, String nodeName, String partitionId, boolean isAntimatter) throws AsterixException {
        String key = STATISTICS_PREFIX + dataset + DELIMITER + indexName + DELIMITER + fieldName + DELIMITER + nodeName
                + DELIMITER + partitionId + DELIMITER + isAntimatter;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }
}
