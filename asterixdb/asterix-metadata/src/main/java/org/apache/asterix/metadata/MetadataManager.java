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

package org.apache.asterix.metadata;

import java.rmi.RemoteException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;
import org.apache.asterix.metadata.api.IExtensionMetadataSearchKey;
import org.apache.asterix.metadata.api.IMetadataManager;
import org.apache.asterix.metadata.api.IMetadataNode;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entities.Statistics;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.transaction.management.service.transaction.JobIdFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.common.ComponentStatisticsId;
import org.apache.hyracks.storage.am.statistics.common.SynopsisFactory;

/**
 * Provides access to Asterix metadata via remote methods to the metadata node.
 * This metadata manager maintains a local cache of metadata Java objects
 * received from the metadata node, to avoid contacting the metadata node
 * repeatedly. We assume that this metadata manager is the only metadata manager
 * in an Asterix cluster. Therefore, no separate cache-invalidation mechanism is
 * needed at this point. Assumptions/Limitations: The metadata subsystem is
 * started during NC Bootstrap start, i.e., when Asterix is deployed. The
 * metadata subsystem is destroyed in NC Bootstrap end, i.e., when Asterix is
 * undeployed. The metadata subsystem consists of the MetadataManager and the
 * MatadataNode. The MetadataManager provides users access to the metadata. The
 * MetadataNode implements direct access to the storage layer on behalf of the
 * MetadataManager, and translates the binary representation of ADM into Java
 * objects for consumption by the MetadataManager's users. There is exactly one
 * instance of the MetadataManager and of the MetadataNode in the cluster, which
 * may or may not be co-located on the same machine (or in the same JVM). The
 * MetadataManager exists in the same JVM as its user's (e.g., the query
 * compiler). The MetadataNode exists in the same JVM as it's transactional
 * components (LockManager, LogManager, etc.) Users shall access the metadata
 * only through the MetadataManager, and never via the MetadataNode directly.
 * Multiple threads may issue requests to the MetadataManager concurrently. For
 * the sake of accessing metadata, we assume a transaction consists of one
 * thread. Users are responsible for locking the metadata (using the
 * MetadataManager API) before issuing requests. The MetadataNode is responsible
 * for acquiring finer-grained locks on behalf of requests from the
 * MetadataManager. Currently, locks are acquired per BTree, since the BTree
 * does not acquire even finer-grained locks yet internally. The metadata can be
 * queried with AQL DML like any other dataset, but can only be changed with AQL
 * DDL. The transaction ids for metadata transactions must be unique across the
 * cluster, i.e., metadata transaction ids shall never "accidentally" overlap
 * with transaction ids of regular jobs or other metadata transactions.
 */
public class MetadataManager implements IMetadataManager {
    private final MetadataCache cache = new MetadataCache();
    protected final IAsterixStateProxy proxy;
    protected IMetadataNode metadataNode;
    private final ReadWriteLock metadataLatch;
    protected boolean rebindMetadataNode = false;

    // TODO(mblow): replace references of this (non-constant) field with a method, update field name accordingly
    public static IMetadataManager INSTANCE;

    private MetadataManager(IAsterixStateProxy proxy, IMetadataNode metadataNode) {
        this(proxy);
        if (metadataNode == null) {
            throw new IllegalArgumentException("Null metadataNode given to MetadataManager");
        }
        this.metadataNode = metadataNode;
    }

    private MetadataManager(IAsterixStateProxy proxy) {
        if (proxy == null) {
            throw new IllegalArgumentException("Null proxy given to MetadataManager");
        }
        this.proxy = proxy;
        this.metadataLatch = new ReentrantReadWriteLock(true);
    }

    @Override
    public void init() throws HyracksDataException {
        GarbageCollector.ensure();
    }

    @Override
    public MetadataTransactionContext beginTransaction() throws RemoteException, ACIDException {
        JobId jobId = JobIdFactory.generateJobId();
        metadataNode.beginTransaction(jobId);
        return new MetadataTransactionContext(jobId);
    }

    @Override
    public void commitTransaction(MetadataTransactionContext ctx) throws RemoteException, ACIDException {
        metadataNode.commitTransaction(ctx.getJobId());
        cache.commit(ctx);
    }

    @Override
    public void abortTransaction(MetadataTransactionContext ctx) throws RemoteException, ACIDException {
        metadataNode.abortTransaction(ctx.getJobId());
    }

    @Override
    public void lock(MetadataTransactionContext ctx, byte lockMode) throws RemoteException, ACIDException {
        metadataNode.lock(ctx.getJobId(), lockMode);
    }

    @Override
    public void unlock(MetadataTransactionContext ctx, byte lockMode) throws RemoteException, ACIDException {
        metadataNode.unlock(ctx.getJobId(), lockMode);
    }

    @Override
    public void addDataverse(MetadataTransactionContext ctx, Dataverse dataverse) throws MetadataException {
        try {
            metadataNode.addDataverse(ctx.getJobId(), dataverse);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addDataverse(dataverse);
    }

    @Override
    public void dropDataverse(MetadataTransactionContext ctx, String dataverseName) throws MetadataException {
        try {
            metadataNode.dropDataverse(ctx.getJobId(), dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropDataverse(dataverseName);
    }

    @Override
    public List<Dataverse> getDataverses(MetadataTransactionContext ctx) throws MetadataException {
        try {
            return metadataNode.getDataverses(ctx.getJobId());
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public Dataverse getDataverse(MetadataTransactionContext ctx, String dataverseName) throws MetadataException {
        // First look in the context to see if this transaction created the
        // requested dataverse itself (but the dataverse is still uncommitted).
        Dataverse dataverse = ctx.getDataverse(dataverseName);
        if (dataverse != null) {
            // Don't add this dataverse to the cache, since it is still
            // uncommitted.
            return dataverse;
        }
        if (ctx.dataverseIsDropped(dataverseName)) {
            // Dataverse has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }
        dataverse = cache.getDataverse(dataverseName);
        if (dataverse != null) {
            // Dataverse is already in the cache, don't add it again.
            return dataverse;
        }
        try {
            dataverse = metadataNode.getDataverse(ctx.getJobId(), dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the dataverse from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (dataverse != null) {
            ctx.addDataverse(dataverse);
        }
        return dataverse;
    }

    @Override
    public List<Dataset> getDataverseDatasets(MetadataTransactionContext ctx, String dataverseName)
            throws MetadataException {
        List<Dataset> dataverseDatasets = new ArrayList<>();
        // add uncommitted temporary datasets
        for (Dataset dataset : ctx.getDataverseDatasets(dataverseName)) {
            if (dataset.getDatasetDetails().isTemp()) {
                dataverseDatasets.add(dataset);
            }
        }
        // add the committed temporary datasets with the cache
        for (Dataset dataset : cache.getDataverseDatasets(dataverseName)) {
            if (dataset.getDatasetDetails().isTemp()) {
                dataverseDatasets.add(dataset);
            }
        }
        try {
            // Assuming that the transaction can read its own writes on the
            // metadata node.
            dataverseDatasets.addAll(metadataNode.getDataverseDatasets(ctx.getJobId(), dataverseName));
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // Don't update the cache to avoid checking against the transaction's
        // uncommitted datasets.
        return dataverseDatasets;
    }

    @Override
    public void addDataset(MetadataTransactionContext ctx, Dataset dataset) throws MetadataException {
        // add dataset into metadataNode
        if (!dataset.getDatasetDetails().isTemp()) {
            try {
                metadataNode.addDataset(ctx.getJobId(), dataset);
            } catch (RemoteException e) {
                throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
            }
        }

        // reflect the dataset into the cache
        ctx.addDataset(dataset);
    }

    @Override
    public void dropDataset(MetadataTransactionContext ctx, String dataverseName, String datasetName)
            throws MetadataException {
        Dataset dataset = findDataset(ctx, dataverseName, datasetName);
        // If a dataset is not in the cache, then it could not be a temp dataset
        if (dataset == null || !dataset.getDatasetDetails().isTemp()) {
            try {
                metadataNode.dropDataset(ctx.getJobId(), dataverseName, datasetName);
            } catch (RemoteException e) {
                throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
            }
        }

        // Drops the dataset from cache
        ctx.dropDataset(dataverseName, datasetName);
    }

    @Override
    public Dataset getDataset(MetadataTransactionContext ctx, String dataverseName, String datasetName)
            throws MetadataException {

        // First look in the context to see if this transaction created the
        // requested dataset itself (but the dataset is still uncommitted).
        Dataset dataset = ctx.getDataset(dataverseName, datasetName);
        if (dataset != null) {
            // Don't add this dataverse to the cache, since it is still
            // uncommitted.
            return dataset;
        }
        if (ctx.datasetIsDropped(dataverseName, datasetName)) {
            // Dataset has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }

        dataset = cache.getDataset(dataverseName, datasetName);
        if (dataset != null) {
            // Dataset is already in the cache, don't add it again.
            return dataset;
        }
        try {
            dataset = metadataNode.getDataset(ctx.getJobId(), dataverseName, datasetName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the dataset from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (dataset != null) {
            ctx.addDataset(dataset);
        }
        return dataset;
    }

    @Override
    public List<Index> getDatasetIndexes(MetadataTransactionContext ctx, String dataverseName, String datasetName)
            throws MetadataException {
        List<Index> datasetIndexes = new ArrayList<>();
        Dataset dataset = findDataset(ctx, dataverseName, datasetName);
        if (dataset == null) {
            return datasetIndexes;
        }
        if (dataset.getDatasetDetails().isTemp()) {
            // for temp datsets
            datasetIndexes = cache.getDatasetIndexes(dataverseName, datasetName);
        } else {
            try {
                // for persistent datasets
                datasetIndexes = metadataNode.getDatasetIndexes(ctx.getJobId(), dataverseName, datasetName);
            } catch (RemoteException e) {
                throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
            }
        }
        return datasetIndexes;
    }

    @Override
    public void addCompactionPolicy(MetadataTransactionContext mdTxnCtx, CompactionPolicy compactionPolicy)
            throws MetadataException {
        try {
            metadataNode.addCompactionPolicy(mdTxnCtx.getJobId(), compactionPolicy);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addCompactionPolicy(compactionPolicy);
    }

    @Override
    public CompactionPolicy getCompactionPolicy(MetadataTransactionContext ctx, String dataverse, String policyName)
            throws MetadataException {

        CompactionPolicy compactionPolicy;
        try {
            compactionPolicy = metadataNode.getCompactionPolicy(ctx.getJobId(), dataverse, policyName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return compactionPolicy;
    }

    @Override
    public void addDatatype(MetadataTransactionContext ctx, Datatype datatype) throws MetadataException {
        try {
            metadataNode.addDatatype(ctx.getJobId(), datatype);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        try {
            ctx.addDatatype(
                    metadataNode.getDatatype(ctx.getJobId(), datatype.getDataverseName(), datatype.getDatatypeName()));
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void dropDatatype(MetadataTransactionContext ctx, String dataverseName, String datatypeName)
            throws MetadataException {
        try {
            metadataNode.dropDatatype(ctx.getJobId(), dataverseName, datatypeName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropDataDatatype(dataverseName, datatypeName);
    }

    @Override
    public Datatype getDatatype(MetadataTransactionContext ctx, String dataverseName, String datatypeName)
            throws MetadataException {
        // First look in the context to see if this transaction created the
        // requested datatype itself (but the datatype is still uncommitted).
        Datatype datatype = ctx.getDatatype(dataverseName, datatypeName);
        if (datatype != null) {
            // Don't add this dataverse to the cache, since it is still
            // uncommitted.
            return datatype;
        }
        if (ctx.datatypeIsDropped(dataverseName, datatypeName)) {
            // Datatype has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }

        datatype = cache.getDatatype(dataverseName, datatypeName);
        if (datatype != null) {
            // Datatype is already in the cache, don't add it again.
            //create a new Datatype object with a new ARecordType object in order to avoid
            //concurrent access to UTF8StringPointable comparator in ARecordType object.
            //see issue 510
            ARecordType aRecType = (ARecordType) datatype.getDatatype();
            return new Datatype(
                    datatype.getDataverseName(), datatype.getDatatypeName(), new ARecordType(aRecType.getTypeName(),
                            aRecType.getFieldNames(), aRecType.getFieldTypes(), aRecType.isOpen()),
                    datatype.getIsAnonymous());
        }
        try {
            datatype = metadataNode.getDatatype(ctx.getJobId(), dataverseName, datatypeName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the datatype from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (datatype != null) {
            ctx.addDatatype(datatype);
        }
        return datatype;
    }

    @Override
    public void addIndex(MetadataTransactionContext ctx, Index index) throws MetadataException {
        String dataverseName = index.getDataverseName();
        String datasetName = index.getDatasetName();
        Dataset dataset = findDataset(ctx, dataverseName, datasetName);
        if (dataset == null || !dataset.getDatasetDetails().isTemp()) {
            try {
                metadataNode.addIndex(ctx.getJobId(), index);
            } catch (RemoteException e) {
                throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
            }
        }
        ctx.addIndex(index);
    }

    @Override
    public void addAdapter(MetadataTransactionContext mdTxnCtx, DatasourceAdapter adapter) throws MetadataException {
        try {
            metadataNode.addAdapter(mdTxnCtx.getJobId(), adapter);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addAdapter(adapter);

    }

    @Override
    public void dropIndex(MetadataTransactionContext ctx, String dataverseName, String datasetName, String indexName)
            throws MetadataException {
        Dataset dataset = findDataset(ctx, dataverseName, datasetName);
        // If a dataset is not in the cache, then it could be an unloaded persistent dataset.
        // If the dataset is a temp dataset, then we do not need to call any MedataNode operations.
        if (dataset == null || !dataset.getDatasetDetails().isTemp()) {
            try {
                metadataNode.dropIndex(ctx.getJobId(), dataverseName, datasetName, indexName);
            } catch (RemoteException e) {
                throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
            }
        }
        ctx.dropIndex(dataverseName, datasetName, indexName);
    }

    @Override
    public Index getIndex(MetadataTransactionContext ctx, String dataverseName, String datasetName, String indexName)
            throws MetadataException {

        // First look in the context to see if this transaction created the
        // requested index itself (but the index is still uncommitted).
        Index index = ctx.getIndex(dataverseName, datasetName, indexName);
        if (index != null) {
            // Don't add this index to the cache, since it is still
            // uncommitted.
            return index;
        }

        if (ctx.indexIsDropped(dataverseName, datasetName, indexName)) {
            // Index has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }

        index = cache.getIndex(dataverseName, datasetName, indexName);
        if (index != null) {
            // Index is already in the cache, don't add it again.
            return index;
        }
        try {
            index = metadataNode.getIndex(ctx.getJobId(), dataverseName, datasetName, indexName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the index from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (index != null) {
            ctx.addIndex(index);
        }
        return index;
    }

    private List<Statistics> getMergedStatistics(MetadataTransactionContext ctx, String dataverseName,
            String datasetName, String indexName, boolean isAntimatter) throws MetadataException {
        List<Statistics> indexStats = getIndexStatistics(ctx, dataverseName, datasetName, indexName, isAntimatter);
        if (!indexStats.isEmpty()) {
            Statistics mergedStats = null;
            boolean isMergeable = true;
            SynopsisType type = null;
            List<ISynopsis<? extends ISynopsisElement>> synopsisList = new ArrayList<>();
            LocalDateTime minComponentId = LocalDateTime.MAX;
            LocalDateTime maxComponentId = LocalDateTime.MIN;
            int synopsisSize = 0;
            int maxSynopsisElements = 0;
            //TODO : proactively merge only stats only within a node/partition?
            for (Statistics stat : indexStats) {
                type = stat.getSynopsis().getType();
                isMergeable &= stat.getSynopsis().isMergeable();
                // Check whether the latest statistic is the merged one
                if (stat.getNode().equals(Statistics.MERGED_STATS_ID)
                        && stat.getPartition().equals(Statistics.MERGED_STATS_ID)) {
                    mergedStats = stat;
                } else {
                    synopsisList.add(stat.getSynopsis());
                    synopsisSize = Integer.max(synopsisSize, stat.getSynopsis().getSize());
                    maxSynopsisElements = Integer.max(maxSynopsisElements, stat.getSynopsis().getElements().size());
                }
                if (stat.getComponentID().getMinTimestamp().isBefore(minComponentId)) {
                    minComponentId = stat.getComponentID().getMinTimestamp();
                }
                if (stat.getComponentID().getMaxTimestamp().isAfter(maxComponentId)) {
                    maxComponentId = stat.getComponentID().getMaxTimestamp();
                }
            }
            if (mergedStats != null && mergedStats.getComponentID().getMinTimestamp().isEqual(minComponentId)
                    && mergedStats.getComponentID().getMaxTimestamp().isEqual(maxComponentId)) {
                List<Statistics> result = new ArrayList(1);
                result.add(mergedStats);
                return result;
            } else {
                if (mergedStats != null) {
                    //invalidate the old merged stats. It was never persisted, hence only remove it from the cache
                    cache.dropStatistics(mergedStats);
                }
                if (maxSynopsisElements > 0 && isMergeable) {
                    Index idx = getIndex(ctx, dataverseName, datasetName, indexName);
                    if (idx.getKeyFieldTypes().size() > 1) {
                        throw new MetadataException("Cannot support statistics on composite fields");
                    }
                    ITypeTraits keyTypeTraits = TypeTraitProvider.INSTANCE.getTypeTrait(idx.getKeyFieldTypes().get(0));

                    try {
                        ISynopsis mergedSynopsis = SynopsisFactory.createSynopsis(type, keyTypeTraits,
                                SynopsisFactory.createSynopsisElements(type, maxSynopsisElements), maxSynopsisElements,
                                synopsisSize);
                        //trigger stats merge routine manually
                        mergedSynopsis.merge(synopsisList);
                        mergedStats = new Statistics(dataverseName, datasetName, indexName, Statistics.MERGED_STATS_ID,
                                Statistics.MERGED_STATS_ID, new ComponentStatisticsId(minComponentId, maxComponentId),
                                true, isAntimatter, mergedSynopsis);
                        //put the merged statistic ONLY into the cache
                        cache.addStatisticsIfNotExists(mergedStats);
                        List<Statistics> result = new ArrayList(1);
                        result.add(mergedStats);
                        return result;
                    } catch (HyracksDataException e) {
                        throw new MetadataException(e);
                    }
                }
            }
        }
        return indexStats;
    }

    @Override
    public List<Statistics> getIndexStatistics(MetadataTransactionContext ctx, String dataverseName, String datasetName,
            String indexName) throws MetadataException {
        List<Statistics> stats = getMergedStatistics(ctx, dataverseName, datasetName, indexName, false);
        stats.addAll(getMergedStatistics(ctx, dataverseName, datasetName, indexName, true));
        return stats;
    }

    @Override
    public List<Statistics> getIndexStatistics(MetadataTransactionContext ctx, String dataverseName, String datasetName,
            String indexName, boolean isAntimatter) throws MetadataException {
        // First look in the context to see if this transaction created the
        // requested statistics itself (but it is still uncommitted).
        List<Statistics> stats = ctx.getIndexStatistics(dataverseName, datasetName, indexName, isAntimatter);
        if (stats != null) {
            // Don't add these statistics to the cache, since they are still uncommitted.
            return stats;
        }

        stats = cache.getIndexStatistics(dataverseName, datasetName, indexName, isAntimatter);
        if (stats != null) {
            // Statistics are already in the cache, don't add it again.
            return stats;
        }
        try {
            stats = metadataNode.getIndexStatistics(ctx.getJobId(), dataverseName, datasetName, indexName,
                    isAntimatter);
        } catch (RemoteException e) {
            throw new MetadataException(e);
        }
        // We fetched the statistics from the MetadataNode. Add it to the current transaction context, so that it gets
        // cached when the transaction commits.
        if (stats != null) {
            for (Statistics s : stats) {
                ctx.addStatistics(s);
            }
        }
        return stats;
    }

    @Override
    public void addNode(MetadataTransactionContext ctx, Node node) throws MetadataException {
        try {
            metadataNode.addNode(ctx.getJobId(), node);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void addNodegroup(MetadataTransactionContext ctx, NodeGroup nodeGroup) throws MetadataException {
        try {
            metadataNode.addNodeGroup(ctx.getJobId(), nodeGroup);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addNodeGroup(nodeGroup);
    }

    @Override
    public void dropNodegroup(MetadataTransactionContext ctx, String nodeGroupName, boolean failSilently)
            throws MetadataException {
        boolean dropped;
        try {
            dropped = metadataNode.dropNodegroup(ctx.getJobId(), nodeGroupName, failSilently);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        if (dropped) {
            ctx.dropNodeGroup(nodeGroupName);
        }
    }

    @Override
    public NodeGroup getNodegroup(MetadataTransactionContext ctx, String nodeGroupName) throws MetadataException {
        // First look in the context to see if this transaction created the
        // requested dataverse itself (but the dataverse is still uncommitted).
        NodeGroup nodeGroup = ctx.getNodeGroup(nodeGroupName);
        if (nodeGroup != null) {
            // Don't add this dataverse to the cache, since it is still
            // uncommitted.
            return nodeGroup;
        }
        if (ctx.nodeGroupIsDropped(nodeGroupName)) {
            // NodeGroup has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }
        nodeGroup = cache.getNodeGroup(nodeGroupName);
        if (nodeGroup != null) {
            // NodeGroup is already in the cache, don't add it again.
            return nodeGroup;
        }
        try {
            nodeGroup = metadataNode.getNodeGroup(ctx.getJobId(), nodeGroupName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the nodeGroup from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (nodeGroup != null) {
            ctx.addNodeGroup(nodeGroup);
        }
        return nodeGroup;
    }

    @Override
    public void addFunction(MetadataTransactionContext mdTxnCtx, Function function) throws MetadataException {
        try {
            metadataNode.addFunction(mdTxnCtx.getJobId(), function);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addFunction(function);
    }

    @Override
    public void dropFunction(MetadataTransactionContext ctx, FunctionSignature functionSignature)
            throws MetadataException {
        try {
            metadataNode.dropFunction(ctx.getJobId(), functionSignature);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropFunction(functionSignature);
    }

    @Override
    public Function getFunction(MetadataTransactionContext ctx, FunctionSignature functionSignature)
            throws MetadataException {
        // First look in the context to see if this transaction created the
        // requested dataset itself (but the dataset is still uncommitted).
        Function function = ctx.getFunction(functionSignature);
        if (function != null) {
            // Don't add this dataverse to the cache, since it is still
            // uncommitted.
            return function;
        }
        if (ctx.functionIsDropped(functionSignature)) {
            // Function has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }
        if (ctx.getDataverse(functionSignature.getNamespace()) != null) {
            // This transaction has dropped and subsequently created the same
            // dataverse.
            return null;
        }
        function = cache.getFunction(functionSignature);
        if (function != null) {
            // Function is already in the cache, don't add it again.
            return function;
        }
        try {
            function = metadataNode.getFunction(ctx.getJobId(), functionSignature);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the function from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (function != null) {
            ctx.addFunction(function);
        }
        return function;

    }

    @Override
    public List<Function> getFunctions(MetadataTransactionContext ctx, String dataverseName) throws MetadataException {
        try {
           return metadataNode.getFunctions(ctx.getJobId(), dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addFeedPolicy(MetadataTransactionContext mdTxnCtx, FeedPolicyEntity feedPolicy)
            throws MetadataException {
        try {
            metadataNode.addFeedPolicy(mdTxnCtx.getJobId(), feedPolicy);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addFeedPolicy(feedPolicy);
    }

    @Override
    public void initializeDatasetIdFactory(MetadataTransactionContext ctx) throws MetadataException {
        try {
            metadataNode.initializeDatasetIdFactory(ctx.getJobId());
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public int getMostRecentDatasetId() throws MetadataException {
        try {
            return metadataNode.getMostRecentDatasetId();
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public List<Function> getDataverseFunctions(MetadataTransactionContext ctx, String dataverseName)
            throws MetadataException {
        List<Function> dataverseFunctions;
        try {
            // Assuming that the transaction can read its own writes on the
            // metadata node.
            dataverseFunctions = metadataNode.getDataverseFunctions(ctx.getJobId(), dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // Don't update the cache to avoid checking against the transaction's
        // uncommitted functions.
        return dataverseFunctions;
    }

    @Override
    public void dropAdapter(MetadataTransactionContext ctx, String dataverseName, String name)
            throws MetadataException {
        try {
            metadataNode.dropAdapter(ctx.getJobId(), dataverseName, name);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public DatasourceAdapter getAdapter(MetadataTransactionContext ctx, String dataverseName, String name)
            throws MetadataException {
        DatasourceAdapter adapter;
        try {
            adapter = metadataNode.getAdapter(ctx.getJobId(), dataverseName, name);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return adapter;
    }

    @Override
    public void dropLibrary(MetadataTransactionContext ctx, String dataverseName, String libraryName)
            throws MetadataException {
        try {
            metadataNode.dropLibrary(ctx.getJobId(), dataverseName, libraryName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropLibrary(dataverseName, libraryName);
    }

    @Override
    public List<Library> getDataverseLibraries(MetadataTransactionContext ctx, String dataverseName)
            throws MetadataException {
        List<Library> dataverseLibaries;
        try {
            // Assuming that the transaction can read its own writes on the
            // metadata node.
            dataverseLibaries = metadataNode.getDataverseLibraries(ctx.getJobId(), dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // Don't update the cache to avoid checking against the transaction's
        // uncommitted functions.
        return dataverseLibaries;
    }

    @Override
    public void addLibrary(MetadataTransactionContext ctx, Library library) throws MetadataException {
        try {
            metadataNode.addLibrary(ctx.getJobId(), library);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addLibrary(library);
    }

    @Override
    public Library getLibrary(MetadataTransactionContext ctx, String dataverseName, String libraryName)
            throws MetadataException, RemoteException {
        Library library;
        try {
            library = metadataNode.getLibrary(ctx.getJobId(), dataverseName, libraryName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return library;
    }

    @Override
    public void acquireWriteLatch() {
        metadataLatch.writeLock().lock();
    }

    @Override
    public void releaseWriteLatch() {
        metadataLatch.writeLock().unlock();
    }

    @Override
    public void acquireReadLatch() {
        metadataLatch.readLock().lock();
    }

    @Override
    public void releaseReadLatch() {
        metadataLatch.readLock().unlock();
    }

    @Override
    public FeedPolicyEntity getFeedPolicy(MetadataTransactionContext ctx, String dataverse, String policyName)
            throws MetadataException {

        FeedPolicyEntity feedPolicy;
        try {
            feedPolicy = metadataNode.getFeedPolicy(ctx.getJobId(), dataverse, policyName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return feedPolicy;
    }

    @Override
    public Feed getFeed(MetadataTransactionContext ctx, String dataverse, String feedName) throws MetadataException {
        Feed feed;
        try {
            feed = metadataNode.getFeed(ctx.getJobId(), dataverse, feedName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return feed;
    }

    @Override
    public List<Feed> getFeeds(MetadataTransactionContext ctx, String dataverse) throws MetadataException {
        List<Feed> feeds;
        try {
            feeds = metadataNode.getFeeds(ctx.getJobId(), dataverse);
        } catch (RemoteException e) {
            throw new MetadataException(e);
        }
        return feeds;
    }

    @Override
    public void dropFeed(MetadataTransactionContext ctx, String dataverse, String feedName) throws MetadataException {
        Feed feed = null;
        List<FeedConnection> feedConnections = null;
        try {
            feed = metadataNode.getFeed(ctx.getJobId(), dataverse, feedName);
            feedConnections = metadataNode.getFeedConnections(ctx.getJobId(), dataverse, feedName);
            metadataNode.dropFeed(ctx.getJobId(), dataverse, feedName);
            for (FeedConnection feedConnection : feedConnections) {
                metadataNode.dropFeedConnection(ctx.getJobId(), dataverse, feedName, feedConnection.getDatasetName());
                ctx.dropFeedConnection(dataverse, feedName, feedConnection.getDatasetName());
            }
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropFeed(feed);
    }

    @Override
    public void addFeed(MetadataTransactionContext ctx, Feed feed) throws MetadataException {
        try {
            metadataNode.addFeed(ctx.getJobId(), feed);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addFeed(feed);
    }

    @Override
    public void addFeedConnection(MetadataTransactionContext ctx, FeedConnection feedConnection)
            throws MetadataException {
        try {
            metadataNode.addFeedConnection(ctx.getJobId(), feedConnection);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addFeedConnection(feedConnection);
    }

    @Override
    public void dropFeedConnection(MetadataTransactionContext ctx, String dataverseName, String feedName,
            String datasetName) throws MetadataException {
        try {
            metadataNode.dropFeedConnection(ctx.getJobId(), dataverseName, feedName, datasetName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropFeedConnection(dataverseName, feedName, datasetName);
    }

    @Override
    public FeedConnection getFeedConnection(MetadataTransactionContext ctx, String dataverseName, String feedName,
            String datasetName) throws MetadataException {
        try {
            return metadataNode.getFeedConnection(ctx.getJobId(), dataverseName, feedName, datasetName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public List<FeedConnection> getFeedConections(MetadataTransactionContext ctx, String dataverseName, String feedName)
            throws MetadataException {
        try {
            return metadataNode.getFeedConnections(ctx.getJobId(), dataverseName, feedName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public List<DatasourceAdapter> getDataverseAdapters(MetadataTransactionContext mdTxnCtx, String dataverse)
            throws MetadataException {
        List<DatasourceAdapter> dataverseAdapters;
        try {
            dataverseAdapters = metadataNode.getDataverseAdapters(mdTxnCtx.getJobId(), dataverse);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return dataverseAdapters;
    }

    @Override
    public void dropFeedPolicy(MetadataTransactionContext mdTxnCtx, String dataverseName, String policyName)
            throws MetadataException {
        FeedPolicyEntity feedPolicy;
        try {
            feedPolicy = metadataNode.getFeedPolicy(mdTxnCtx.getJobId(), dataverseName, policyName);
            metadataNode.dropFeedPolicy(mdTxnCtx.getJobId(), dataverseName, policyName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.dropFeedPolicy(feedPolicy);
    }

    public List<FeedPolicyEntity> getDataversePolicies(MetadataTransactionContext mdTxnCtx, String dataverse)
            throws MetadataException {
        List<FeedPolicyEntity> dataverseFeedPolicies;
        try {
            dataverseFeedPolicies = metadataNode.getDataversePolicies(mdTxnCtx.getJobId(), dataverse);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return dataverseFeedPolicies;
    }

    @Override
    public List<ExternalFile> getDatasetExternalFiles(MetadataTransactionContext mdTxnCtx, Dataset dataset)
            throws MetadataException {
        List<ExternalFile> externalFiles;
        try {
            externalFiles = metadataNode.getExternalFiles(mdTxnCtx.getJobId(), dataset);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return externalFiles;
    }

    @Override
    public void addExternalFile(MetadataTransactionContext ctx, ExternalFile externalFile) throws MetadataException {
        try {
            metadataNode.addExternalFile(ctx.getJobId(), externalFile);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void dropExternalFile(MetadataTransactionContext ctx, ExternalFile externalFile) throws MetadataException {
        try {
            metadataNode.dropExternalFile(ctx.getJobId(), externalFile.getDataverseName(),
                    externalFile.getDatasetName(), externalFile.getFileNumber());
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public ExternalFile getExternalFile(MetadataTransactionContext ctx, String dataverseName, String datasetName,
            Integer fileNumber) throws MetadataException {
        ExternalFile file;
        try {
            file = metadataNode.getExternalFile(ctx.getJobId(), dataverseName, datasetName, fileNumber);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return file;
    }

    //TODO: Optimize <-- use keys instead of object -->
    @Override
    public void dropDatasetExternalFiles(MetadataTransactionContext mdTxnCtx, Dataset dataset)
            throws MetadataException {
        try {
            metadataNode.dropExternalFiles(mdTxnCtx.getJobId(), dataset);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void addStatistics(MetadataTransactionContext mdTxnCtx, Statistics statistics) throws MetadataException {
        if (!statistics.isTemp()) {
            try {
                metadataNode.addStatistics(mdTxnCtx.getJobId(), statistics);
            } catch (RemoteException e) {
                throw new MetadataException(e);
            }
        }
        mdTxnCtx.addStatistics(statistics);
    }

    private Statistics findStatistics(MetadataTransactionContext ctx, String dataverseName, String datasetName,
            String indexName, String node, String partition, ComponentStatisticsId componentId, boolean isAntimatter) {
        Statistics stats =
                ctx.getStatistics(dataverseName, datasetName, indexName, node, partition, componentId, isAntimatter);
        if (stats == null) {
            stats = cache.getStatistics(dataverseName, datasetName, indexName, node, partition, componentId,
                    isAntimatter);
        }
        return stats;
    }

    @Override
    public void dropStatistics(MetadataTransactionContext ctx, String dataverseName, String datasetName,
            String indexName, String node, String partition, ComponentStatisticsId componentId, boolean isAntimatter)
            throws MetadataException {
        Statistics stat =
                findStatistics(ctx, dataverseName, datasetName, indexName, node, partition, componentId, isAntimatter);
        if (stat == null || !stat.isTemp()) {
            try {
                metadataNode.dropStatistics(ctx.getJobId(), dataverseName, datasetName, indexName, node, partition,
                        componentId, isAntimatter);
            } catch (RemoteException e) {
                throw new MetadataException(e);
            }
        }
        // Drops the stat from cache
        ctx.dropStatistics(dataverseName, datasetName, indexName, isAntimatter, node, partition, componentId);
    }

    @Override
    public void updateDataset(MetadataTransactionContext ctx, Dataset dataset) throws MetadataException {
        try {
            metadataNode.updateDataset(ctx.getJobId(), dataset);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // reflect the dataset into the cache
        ctx.dropDataset(dataset.getDataverseName(), dataset.getDatasetName());
        ctx.addDataset(dataset);
    }

    @Override
    public void cleanupTempDatasets() {
        cache.cleanupTempDatasets();
    }

    public Dataset findDataset(MetadataTransactionContext ctx, String dataverseName, String datasetName) {
        Dataset dataset = ctx.getDataset(dataverseName, datasetName);
        if (dataset == null) {
            dataset = cache.getDataset(dataverseName, datasetName);
        }
        return dataset;
    }

    @Override
    public <T extends IExtensionMetadataEntity> void addEntity(MetadataTransactionContext mdTxnCtx, T entity)
            throws MetadataException {
        try {
            metadataNode.addEntity(mdTxnCtx.getJobId(), entity);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public <T extends IExtensionMetadataEntity> void upsertEntity(MetadataTransactionContext mdTxnCtx, T entity)
            throws MetadataException {
        try {
            metadataNode.upsertEntity(mdTxnCtx.getJobId(), entity);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public <T extends IExtensionMetadataEntity> void deleteEntity(MetadataTransactionContext mdTxnCtx, T entity)
            throws MetadataException {
        try {
            metadataNode.deleteEntity(mdTxnCtx.getJobId(), entity);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public <T extends IExtensionMetadataEntity> List<T> getEntities(MetadataTransactionContext mdTxnCtx,
            IExtensionMetadataSearchKey searchKey) throws MetadataException {
        try {
            return metadataNode.getEntities(mdTxnCtx.getJobId(), searchKey);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void rebindMetadataNode() {
        rebindMetadataNode = true;
    }

    public static void initialize(IAsterixStateProxy proxy, MetadataProperties metadataProperties) {
        INSTANCE = new CCMetadataManagerImpl(proxy, metadataProperties);
    }

    public static void initialize(IAsterixStateProxy proxy, MetadataNode metadataNode) {
        INSTANCE = new MetadataManager(proxy, metadataNode);
    }

    private static class CCMetadataManagerImpl extends MetadataManager {
        private final MetadataProperties metadataProperties;

        public CCMetadataManagerImpl(IAsterixStateProxy proxy, MetadataProperties metadataProperties) {
            super(proxy);
            this.metadataProperties = metadataProperties;
        }

        @Override
        public synchronized void init() throws HyracksDataException {
            if (metadataNode != null && !rebindMetadataNode) {
                return;
            }
            try {
                metadataNode =
                        proxy.waitForMetadataNode(metadataProperties.getRegistrationTimeoutSecs(), TimeUnit.SECONDS);
                if (metadataNode != null) {
                    rebindMetadataNode = false;
                } else {
                    throw new HyracksDataException("The MetadataNode failed to bind before the configured timeout ("
                            + metadataProperties.getRegistrationTimeoutSecs() + " seconds); the MetadataNode was "
                            + "configured to run on NC: " + metadataProperties.getMetadataNodeName());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            } catch (RemoteException e) {
                throw new RuntimeDataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
            }
            super.init();
        }
    }
}
