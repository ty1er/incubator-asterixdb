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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * Operator subtree that matches the following patterns, and provides convenient access to its nodes:
 * (select)? <-- (assign | unnest)* <-- (datasource scan | unnest-map)*
 */
public class OptimizableOperatorSubTree {

    public static enum DataSourceType {
        DATASOURCE_SCAN,
        EXTERNAL_SCAN,
        PRIMARY_INDEX_LOOKUP,
        COLLECTION_SCAN,
        INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP,
        NO_DATASOURCE
    }

    private ILogicalOperator root = null;
    private Mutable<ILogicalOperator> rootRef = null;
    private final List<Mutable<ILogicalOperator>> assignsAndUnnestsRefs = new ArrayList<>();
    private final List<AbstractLogicalOperator> assignsAndUnnests = new ArrayList<>();
    private Mutable<ILogicalOperator> dataSourceRef = null;
    private DataSourceType dataSourceType = DataSourceType.NO_DATASOURCE;

    // Dataset and type metadata. Set in setDatasetAndTypeMetadata().
    private Dataset dataset = null;
    private ARecordType recordType = null;
    private ARecordType metaRecordType = null;
    // Contains the field names for all assign operations in this sub-tree.
    // This will be used for the index-only plan check.
    private Map<LogicalVariable, List<String>> varsTofieldNameMap = new HashMap<>();

    // Additional datasources can exist if IntroduceJoinAccessMethodRule has been applied.
    // (E.g. There are index-nested-loop-joins in the plan.)
    private List<Mutable<ILogicalOperator>> ixJoinOuterAdditionalDataSourceRefs = null;
    private List<DataSourceType> ixJoinOuterAdditionalDataSourceTypes = null;
    private List<Dataset> ixJoinOuterAdditionalDatasets = null;
    private List<ARecordType> ixJoinOuterAdditionalRecordTypes = null;

    /**
     * Identifies the root of the subtree and initializes the data-source, assign, and unnest information.
     */
    public boolean initFromSubTree(Mutable<ILogicalOperator> subTreeOpRef, IOptimizationContext context)
            throws AlgebricksException {
        reset();
        rootRef = subTreeOpRef;
        root = subTreeOpRef.getValue();

        boolean passedSource = false;
        boolean result = false;
        Mutable<ILogicalOperator> searchOpRef = subTreeOpRef;
        // Examine the op's children to match the expected patterns.
        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) searchOpRef.getValue();
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();

        do {
            // Skips the limit operator.
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.LIMIT) {
                searchOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) searchOpRef.getValue();
            }
            // Match (assign | unnest)*.
            while (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    || subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                assignsAndUnnestsRefs.add(searchOpRef);
                assignsAndUnnests.add(subTreeOp);
                searchOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) searchOpRef.getValue();
            }
            // Skips Order by information.
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ORDER) {
                searchOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) searchOpRef.getValue();
            }
            // If this is an index-only plan, then we have UNIONALL operator.
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                // If this plan has a UNIONALL operator and turns out to be a non-index only plan,
                // then we try to initialize data source and return to the caller.
                if (!initFromIndexOnlyPlan(searchOpRef)) {
                    return initializeDataSource(searchOpRef);
                } else {
                    // Done initializing the data source in an index-only plan.
                    return true;
                }
            }
            // Skips select operator.
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                searchOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) searchOpRef.getValue();
            }
            // Match datasource information if there are no (assign | unnest)*
            if (subTreeOp.getOperatorTag() != LogicalOperatorTag.ASSIGN
                    && subTreeOp.getOperatorTag() != LogicalOperatorTag.UNNEST) {
                // Pattern may still match if we are looking for primary index matches as well.
                result = initializeDataSource(searchOpRef);
                passedSource = true;
                if (!subTreeOp.getInputs().isEmpty()) {
                    searchOpRef = subTreeOp.getInputs().get(0);
                    subTreeOp = (AbstractLogicalOperator) searchOpRef.getValue();
                }
            }
            // Match (assign | unnest)+.
            while (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    || subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                if (!passedSource && !OperatorPropertiesUtil.isMovable(subTreeOp)) {
                    return false;
                }
                if (subTreeOp.getExecutionMode() != ExecutionMode.UNPARTITIONED) {
                    //The unpartitioned ops should stay below the search
                    assignsAndUnnestsRefs.add(searchOpRef);
                }
                assignsAndUnnests.add(subTreeOp);

                searchOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) searchOpRef.getValue();
            }
            // Gathers the variable to field-name mapping information for (unnest-map)?
            // This information is used to decide whether the given plan is an index-only plan or not.
            // We do not change the given subTreeOpRef at this point to properly setup the data-source.
            Mutable<ILogicalOperator> additionalSubTreeOpRef;
            while (subTreeOp.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
                if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP
                        || subTreeOp.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
                    gatherAccessMethodJobGenParamAndSetFieldNameForIndexSearch(subTreeOp, metadataProvider);
                }
                additionalSubTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) additionalSubTreeOpRef.getValue();

                if (subTreeOp.getInputs().isEmpty()) {
                    break;
                }
            }

            if (passedSource) {
                return result;
            }
        } while (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT);

        // Match data source (datasource scan or primary index search).
        return initializeDataSource(searchOpRef);
    }

    /**
     * Gather AccessMethodJobGenParam for unnest-map (or left-outer-unnest-map) operator and
     * call setVarToFieldNameMappingFromUnnestMap to set variable to field name mapping from this operator.
     * This information is used to decide whether the given plan is an index-only plan or not.
     */
    private void gatherAccessMethodJobGenParamAndSetFieldNameForIndexSearch(ILogicalOperator indexSearchOp,
            MetadataProvider metadataProvider) throws AlgebricksException {
        UnnestMapOperator unnestMapOp = null;
        LeftOuterUnnestMapOperator leftOuterUnnestMapOp = null;
        List<LogicalVariable> unnestMapOpVars = null;
        ILogicalExpression unnestExpr = null;
        AbstractFunctionCallExpression unnestFuncExpr = null;
        FunctionIdentifier funcIdent = null;

        if (indexSearchOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            unnestMapOp = (UnnestMapOperator) indexSearchOp;
            unnestMapOpVars = unnestMapOp.getVariables();
            unnestExpr = unnestMapOp.getExpressionRef().getValue();
            unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
            funcIdent = unnestFuncExpr.getFunctionIdentifier();
            if (funcIdent.equals(BuiltinFunctions.INDEX_SEARCH)) {
                AccessMethodJobGenParams jobGenParams =
                        AbstractIntroduceAccessMethodRule.getAccessMethodJobGenParamsFromUnnestMap(unnestMapOp);
                setVarToFieldNameMappingFromUnnestMap(jobGenParams, unnestMapOpVars, metadataProvider);
            }
        } else if (indexSearchOp.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
            leftOuterUnnestMapOp = (LeftOuterUnnestMapOperator) indexSearchOp;
            unnestMapOpVars = leftOuterUnnestMapOp.getVariables();
            unnestExpr = leftOuterUnnestMapOp.getExpressionRef().getValue();
            unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
            funcIdent = unnestFuncExpr.getFunctionIdentifier();
            if (funcIdent.equals(BuiltinFunctions.INDEX_SEARCH)) {
                AccessMethodJobGenParams jobGenParams = AbstractIntroduceAccessMethodRule
                        .getAccessMethodJobGenParamsFromUnnestMap(leftOuterUnnestMapOp);
                setVarToFieldNameMappingFromUnnestMap(jobGenParams, unnestMapOpVars, metadataProvider);
            }
        }
    }

    private boolean initializeDataSource(Mutable<ILogicalOperator> subTreeOpRef) {
        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();

        if (subTreeOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            setDataSourceType(DataSourceType.DATASOURCE_SCAN);
            setDataSourceRef(subTreeOpRef);
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            setDataSourceType(DataSourceType.COLLECTION_SCAN);
            setDataSourceRef(subTreeOpRef);
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            // There can be multiple unnest-map or datasource-scan operators
            // if index-nested-loop-join has been applied by IntroduceJoinAccessMethodRule.
            // So, we need to traverse the whole path from the subTreeOp.
            boolean dataSourceFound = false;
            while (true) {
                if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                    UnnestMapOperator unnestMapOp = (UnnestMapOperator) subTreeOp;
                    ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();

                    if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                        if (f.getFunctionIdentifier().equals(BuiltinFunctions.INDEX_SEARCH)) {
                            AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                            jobGenParams.readFromFuncArgs(f.getArguments());
                            if (jobGenParams.isPrimaryIndex()) {
                                if (getDataSourceRef() == null) {
                                    setDataSourceRef(subTreeOpRef);
                                    setDataSourceType(DataSourceType.PRIMARY_INDEX_LOOKUP);
                                } else {
                                    // One datasource already exists. This is an additional datasource.
                                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                                    getIxJoinOuterAdditionalDataSourceTypes().add(DataSourceType.PRIMARY_INDEX_LOOKUP);
                                    getIxJoinOuterAdditionalDataSourceRefs().add(subTreeOpRef);
                                }
                                dataSourceFound = true;
                            } else {
                                // Secondary index search in an index-only plan case
                                if (getDataSourceRef() == null) {
                                    setDataSourceRef(subTreeOpRef);
                                    setDataSourceType(DataSourceType.INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP);
                                } else {
                                    // One datasource already exists. This is an additional datasource.
                                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                                    ixJoinOuterAdditionalDataSourceTypes
                                            .add(DataSourceType.INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP);
                                    ixJoinOuterAdditionalDataSourceRefs.add(subTreeOpRef);
                                }
                                dataSourceFound = true;
                            }
                        } else if (f.getFunctionIdentifier().equals(BuiltinFunctions.EXTERNAL_LOOKUP)) {
                            // External lookup case
                            if (getDataSourceRef() == null) {
                                setDataSourceRef(subTreeOpRef);
                                setDataSourceType(DataSourceType.EXTERNAL_SCAN);
                            } else {
                                // One datasource already exists. This is an additional datasource.
                                initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                                getIxJoinOuterAdditionalDataSourceTypes().add(DataSourceType.EXTERNAL_SCAN);
                                getIxJoinOuterAdditionalDataSourceRefs().add(subTreeOpRef);
                            }
                            dataSourceFound = true;
                        }
                    }
                } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                    getIxJoinOuterAdditionalDataSourceTypes().add(DataSourceType.DATASOURCE_SCAN);
                    getIxJoinOuterAdditionalDataSourceRefs().add(subTreeOpRef);
                    dataSourceFound = true;
                } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                    getIxJoinOuterAdditionalDataSourceTypes().add(DataSourceType.COLLECTION_SCAN);
                    getIxJoinOuterAdditionalDataSourceRefs().add(subTreeOpRef);
                }

                // Traverse the subtree while there are operators in the path.
                if (subTreeOp.hasInputs()) {
                    subTreeOpRef = subTreeOp.getInputs().get(0);
                    subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
                } else {
                    break;
                }
            }

            if (dataSourceFound) {
                return true;
            }
        }

        return false;
    }

    /**
     * Initializes assign, unnest and datasource information for the given index-only plan.
     *
     * @throws AlgebricksException
     */
    private boolean initFromIndexOnlyPlan(Mutable<ILogicalOperator> subTreeOpRef) throws AlgebricksException {
        Mutable<ILogicalOperator> opRef = subTreeOpRef;
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Top level operator should be UNIONALL operator.
        if (op.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
            return false;
        }
        UnionAllOperator unionAllOp = (UnionAllOperator) op;

        // The complete path
        //
        // Index-only plan path:
        // Left branch -
        // ... < UNION < PROJECT < SELECT < ASSIGN? < UNNEST_MAP(PIdx) < SPLIT < UNNEST_MAP (SIdx) < ASSIGN? < ...
        // Right branch -
        //             < PROJECT < SELECT? < ASSIGN?                   <                      ""

        // We now traverse the left path first (instantTryLock on PK fail path).
        // Index-only plan: PROJECT
        opRef = op.getInputs().get(0);
        op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            return false;
        }

        // The left path
        // Index-only plan: SELECT
        opRef = op.getInputs().get(0);
        op = (AbstractLogicalOperator) opRef.getValue();

        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        // The left path
        // Index-only plan: ASSIGN? or UNNEST?
        opRef = op.getInputs().get(0);
        op = (AbstractLogicalOperator) opRef.getValue();
        while (op.getOperatorTag() == LogicalOperatorTag.ASSIGN || op.getOperatorTag() == LogicalOperatorTag.UNNEST) {
            assignsAndUnnestsRefs.add(opRef);
            assignsAndUnnests.add(op);
            opRef = op.getInputs().get(0);
            op = (AbstractLogicalOperator) opRef.getValue();
        }

        // The left path
        // Index-only plan: UNNEST-MAP (PIdx search)
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST_MAP) {
            return false;
        }

        // The left path
        // Index-only plan: SPLIT - left path is done
        opRef = op.getInputs().get(0);
        op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SPLIT) {
            return false;
        }

        // The left path is done. Now, checks the right branch of the UNIONALL operator.
        // Index-only plan right path:
        //   UNION <- PROJECT <- SELECT? <- ASSIGN? <- SPLIT <- ...

        // The right path
        // Index-only plan: PROJECT
        opRef = unionAllOp.getInputs().get(1);
        op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            return false;
        }

        // The right path
        // Index-only plan: SELECT?
        opRef = op.getInputs().get(0);
        op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            // This is actually an R-Tree index search case
            // since we might need to do a post-verification.
            opRef = op.getInputs().get(0);
            op = (AbstractLogicalOperator) opRef.getValue();
        }

        // Index-only plan: ASSIGN?
        while (op.getOperatorTag() == LogicalOperatorTag.ASSIGN || op.getOperatorTag() == LogicalOperatorTag.UNNEST) {
            assignsAndUnnestsRefs.add(opRef);
            assignsAndUnnests.add(op);
            opRef = op.getInputs().get(0);
            op = (AbstractLogicalOperator) opRef.getValue();
        }

        // Index-only plan: SPLIT - right path is done
        // Reducing the number of SELECT verification plan: already done
        if (op.getOperatorTag() != LogicalOperatorTag.SPLIT) {
            return false;
        }

        // Now, we traversed both the left and the right path.
        // We are going to traverse the common path before the SPLIT operator.
        // Index-only plan common path:
        //  ... <- SPLIT <- UNNEST_MAP (SIdx) <- ASSIGN? <- ...

        // Index-only plan: UNNEST-MAP (SIdx)
        opRef = op.getInputs().get(0);
        op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST_MAP) {
            return false;
        }
        // This secondary index search is the data source that we can trust.
        boolean initDataSource = initializeDataSource(opRef);
        if (!initDataSource) {
            return false;
        }

        // Index-only plan: ASSIGN?
        opRef = op.getInputs().get(0);
        op = (AbstractLogicalOperator) opRef.getValue();
        while (op.getOperatorTag() == LogicalOperatorTag.ASSIGN || op.getOperatorTag() == LogicalOperatorTag.UNNEST) {
            assignsAndUnnestsRefs.add(opRef);
            assignsAndUnnests.add(op);
            opRef = op.getInputs().get(0);
            op = (AbstractLogicalOperator) opRef.getValue();
        }

        // Index-only plan: EMPTYTUPLESOURCE
        // Reducing the number of the SELECT verification plan: ORDER?
        if (op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            return false;
        }

        return true;
    }

    /**
     * Find the dataset corresponding to the datasource scan in the metadata.
     * Also sets recordType to be the type of that dataset.
     */
    public boolean setDatasetAndTypeMetadata(MetadataProvider metadataProvider) throws AlgebricksException {
        String dataverseName = null;
        String datasetName = null;

        Dataset ds = null;
        ARecordType rType = null;

        List<Mutable<ILogicalOperator>> sourceOpRefs = new ArrayList<>();
        List<DataSourceType> dsTypes = new ArrayList<>();

        sourceOpRefs.add(getDataSourceRef());
        dsTypes.add(getDataSourceType());

        // If there are multiple datasources in the subtree, we need to find the dataset for these.
        if (getIxJoinOuterAdditionalDataSourceRefs() != null) {
            for (int i = 0; i < getIxJoinOuterAdditionalDataSourceRefs().size(); i++) {
                sourceOpRefs.add(getIxJoinOuterAdditionalDataSourceRefs().get(i));
                dsTypes.add(getIxJoinOuterAdditionalDataSourceTypes().get(i));
            }
        }

        for (int i = 0; i < sourceOpRefs.size(); i++) {
            switch (dsTypes.get(i)) {
                case DATASOURCE_SCAN:
                    DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) sourceOpRefs.get(i).getValue();
                    IDataSource<?> datasource = dataSourceScan.getDataSource();
                    if (datasource instanceof DataSource) {
                        byte dsType = ((DataSource) datasource).getDatasourceType();
                        if (dsType != DataSource.Type.INTERNAL_DATASET && dsType != DataSource.Type.EXTERNAL_DATASET) {
                            return false;
                        }
                    }
                    Pair<String, String> datasetInfo = AnalysisUtil.getDatasetInfo(dataSourceScan);
                    dataverseName = datasetInfo.first;
                    datasetName = datasetInfo.second;
                    break;
                case PRIMARY_INDEX_LOOKUP:
                case INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP:
                    AbstractUnnestOperator unnestMapOp = (AbstractUnnestOperator) sourceOpRefs.get(i).getValue();
                    ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
                    AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                    AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                    jobGenParams.readFromFuncArgs(f.getArguments());
                    datasetName = jobGenParams.getDatasetName();
                    dataverseName = jobGenParams.getDataverseName();
                    break;
                case EXTERNAL_SCAN:
                    UnnestMapOperator externalScan = (UnnestMapOperator) sourceOpRefs.get(i).getValue();
                    datasetInfo = AnalysisUtil.getExternalDatasetInfo(externalScan);
                    dataverseName = datasetInfo.first;
                    datasetName = datasetInfo.second;
                    break;
                case COLLECTION_SCAN:
                    if (i != 0) {
                        getIxJoinOuterAdditionalDatasets().add(null);
                        getIxJoinOuterAdditionalRecordTypes().add(null);
                    }
                    continue;
                case NO_DATASOURCE:
                default:
                    return false;
            }
            if (dataverseName == null || datasetName == null) {
                return false;
            }
            // Find the dataset corresponding to the datasource in the metadata.
            ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                throw CompilationException.create(ErrorCode.NO_METADATA_FOR_DATASET, datasetName);
            }
            // Get the record type for that dataset.
            IAType itemType = metadataProvider.findType(ds.getItemTypeDataverseName(), ds.getItemTypeName());
            if (itemType.getTypeTag() != ATypeTag.OBJECT) {
                if (i == 0) {
                    return false;
                } else {
                    getIxJoinOuterAdditionalDatasets().add(null);
                    getIxJoinOuterAdditionalRecordTypes().add(null);
                }
            }
            rType = (ARecordType) itemType;

            // Get the meta record type for that dataset.
            IAType metaItemType =
                    metadataProvider.findType(ds.getMetaItemTypeDataverseName(), ds.getMetaItemTypeName());

            // First index is always the primary datasource in this subtree.
            if (i == 0) {
                setDataset(ds);
                setRecordType(rType);
                setMetaRecordType((ARecordType) metaItemType);
            } else {
                getIxJoinOuterAdditionalDatasets().add(ds);
                getIxJoinOuterAdditionalRecordTypes().add(rType);
            }

            dataverseName = null;
            datasetName = null;
            ds = null;
            rType = null;
        }

        return true;
    }

    public boolean hasDataSource() {
        return getDataSourceType() != DataSourceType.NO_DATASOURCE;
    }

    public boolean hasIxJoinOuterAdditionalDataSource() {
        boolean dataSourceFound = false;
        if (getIxJoinOuterAdditionalDataSourceTypes() != null) {
            for (int i = 0; i < getIxJoinOuterAdditionalDataSourceTypes().size(); i++) {
                if (getIxJoinOuterAdditionalDataSourceTypes().get(i) != DataSourceType.NO_DATASOURCE) {
                    dataSourceFound = true;
                    break;
                }
            }
        }
        return dataSourceFound;
    }

    public boolean hasDataSourceScan() {
        return getDataSourceType() == DataSourceType.DATASOURCE_SCAN;
    }

    public boolean hasIxJoinOuterAdditionalDataSourceScan() {
        if (getIxJoinOuterAdditionalDataSourceTypes() != null) {
            for (int i = 0; i < getIxJoinOuterAdditionalDataSourceTypes().size(); i++) {
                if (getIxJoinOuterAdditionalDataSourceTypes().get(i) == DataSourceType.DATASOURCE_SCAN) {
                    return true;
                }
            }
        }
        return false;
    }

    public void reset() {
        setRoot(null);
        setRootRef(null);
        getAssignsAndUnnestsRefs().clear();
        getAssignsAndUnnests().clear();
        setDataSourceRef(null);
        setDataSourceType(DataSourceType.NO_DATASOURCE);
        setIxJoinOuterAdditionalDataSourceRefs(null);
        setIxJoinOuterAdditionalDataSourceTypes(null);
        setDataset(null);
        setIxJoinOuterAdditionalDatasets(null);
        setRecordType(null);
        setIxJoinOuterAdditionalRecordTypes(null);
    }

    /**
     * Gets the primary key variables from the given data-source.
     */
    public void getPrimaryKeyVars(Mutable<ILogicalOperator> dataSourceRefToFetch, List<LogicalVariable> target)
            throws AlgebricksException {
        Mutable<ILogicalOperator> dataSourceRefToFetchKey =
                (dataSourceRefToFetch == null) ? dataSourceRef : dataSourceRefToFetch;
        switch (dataSourceType) {
            case DATASOURCE_SCAN:
                DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) getDataSourceRef().getValue();
                int numPrimaryKeys = dataset.getPrimaryKeys().size();
                for (int i = 0; i < numPrimaryKeys; i++) {
                    target.add(dataSourceScan.getVariables().get(i));
                }
                break;
            case PRIMARY_INDEX_LOOKUP:
                AbstractUnnestMapOperator unnestMapOp = (AbstractUnnestMapOperator) dataSourceRefToFetchKey.getValue();
                List<LogicalVariable> primaryKeys = null;
                primaryKeys = AccessMethodUtils.getPrimaryKeyVarsFromPrimaryUnnestMap(dataset, unnestMapOp);
                target.addAll(primaryKeys);
                break;
            case INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP:
                AbstractUnnestMapOperator idxOnlyPlanUnnestMapOp =
                        (AbstractUnnestMapOperator) dataSourceRefToFetchKey.getValue();
                List<LogicalVariable> idxOnlyPlanKeyVars = idxOnlyPlanUnnestMapOp.getVariables();
                int indexOnlyPlanNumPrimaryKeys = dataset.getPrimaryKeys().size();
                // The order of variables: SK, PK, the result of instantTryLock on PK.
                // The last variable keeps the result of instantTryLock on PK.
                // Thus, we deduct 1 to only count key variables.
                int start = idxOnlyPlanKeyVars.size() - 1 - indexOnlyPlanNumPrimaryKeys;
                int end = start + indexOnlyPlanNumPrimaryKeys;

                for (int i = start; i < end; i++) {
                    target.add(idxOnlyPlanKeyVars.get(i));
                }
                break;
            case EXTERNAL_SCAN:
                break;
            case NO_DATASOURCE:
            default:
                throw CompilationException.create(ErrorCode.SUBTREE_HAS_NO_DATA_SOURCE);
        }
    }

    public List<LogicalVariable> getDataSourceVariables() throws AlgebricksException {
        switch (getDataSourceType()) {
            case DATASOURCE_SCAN:
            case EXTERNAL_SCAN:
            case PRIMARY_INDEX_LOOKUP:
                AbstractScanOperator scanOp = (AbstractScanOperator) getDataSourceRef().getValue();
                return scanOp.getVariables();
            case INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP:
                // This data-source doesn't have record variables.
                List<LogicalVariable> pkVars = new ArrayList<>();
                getPrimaryKeyVars(dataSourceRef, pkVars);
                return pkVars;
            case COLLECTION_SCAN:
                return new ArrayList<>();
            case NO_DATASOURCE:
            default:
                throw CompilationException.create(ErrorCode.SUBTREE_HAS_NO_DATA_SOURCE);
        }
    }

    public List<LogicalVariable> getIxJoinOuterAdditionalDataSourceVariables(int idx) throws AlgebricksException {
        if (getIxJoinOuterAdditionalDataSourceRefs() != null && getIxJoinOuterAdditionalDataSourceRefs().size() > idx) {
            switch (getIxJoinOuterAdditionalDataSourceTypes().get(idx)) {
                case DATASOURCE_SCAN:
                case EXTERNAL_SCAN:
                case PRIMARY_INDEX_LOOKUP:
                    AbstractScanOperator scanOp =
                            (AbstractScanOperator) getIxJoinOuterAdditionalDataSourceRefs().get(idx).getValue();
                    return scanOp.getVariables();
                case INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP:
                    List<LogicalVariable> PKVars = new ArrayList<>();
                    getPrimaryKeyVars(ixJoinOuterAdditionalDataSourceRefs.get(idx), PKVars);
                    return PKVars;
                case COLLECTION_SCAN:
                    return new ArrayList<>();
                case NO_DATASOURCE:
                default:
                    throw CompilationException.create(ErrorCode.SUBTREE_HAS_NO_ADDTIONAL_DATA_SOURCE);
            }
        } else {
            return null;
        }
    }

    public void initializeIxJoinOuterAddtionalDataSourcesIfEmpty() {
        if (getIxJoinOuterAdditionalDataSourceRefs() == null) {
            setIxJoinOuterAdditionalDataSourceRefs(new ArrayList<Mutable<ILogicalOperator>>());
            setIxJoinOuterAdditionalDataSourceTypes(new ArrayList<DataSourceType>());
            setIxJoinOuterAdditionalDatasets(new ArrayList<Dataset>());
            setIxJoinOuterAdditionalRecordTypes(new ArrayList<ARecordType>());
        }
    }

    public ILogicalOperator getRoot() {
        return root;
    }

    public void setRoot(ILogicalOperator root) {
        this.root = root;
    }

    public Mutable<ILogicalOperator> getRootRef() {
        return rootRef;
    }

    public void setRootRef(Mutable<ILogicalOperator> rootRef) {
        this.rootRef = rootRef;
    }

    public List<Mutable<ILogicalOperator>> getAssignsAndUnnestsRefs() {
        return assignsAndUnnestsRefs;
    }

    public List<AbstractLogicalOperator> getAssignsAndUnnests() {
        return assignsAndUnnests;
    }

    public Mutable<ILogicalOperator> getDataSourceRef() {
        return dataSourceRef;
    }

    public void setDataSourceRef(Mutable<ILogicalOperator> dataSourceRef) {
        this.dataSourceRef = dataSourceRef;
    }

    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    public ARecordType getMetaRecordType() {
        return metaRecordType;
    }

    public void setMetaRecordType(ARecordType metaRecordType) {
        this.metaRecordType = metaRecordType;
    }

    public List<Mutable<ILogicalOperator>> getIxJoinOuterAdditionalDataSourceRefs() {
        return ixJoinOuterAdditionalDataSourceRefs;
    }

    public void setIxJoinOuterAdditionalDataSourceRefs(
            List<Mutable<ILogicalOperator>> ixJoinOuterAdditionalDataSourceRefs) {
        this.ixJoinOuterAdditionalDataSourceRefs = ixJoinOuterAdditionalDataSourceRefs;
    }

    public List<DataSourceType> getIxJoinOuterAdditionalDataSourceTypes() {
        return ixJoinOuterAdditionalDataSourceTypes;
    }

    public void setIxJoinOuterAdditionalDataSourceTypes(List<DataSourceType> ixJoinOuterAdditionalDataSourceTypes) {
        this.ixJoinOuterAdditionalDataSourceTypes = ixJoinOuterAdditionalDataSourceTypes;
    }

    public List<Dataset> getIxJoinOuterAdditionalDatasets() {
        return ixJoinOuterAdditionalDatasets;
    }

    public void setIxJoinOuterAdditionalDatasets(List<Dataset> ixJoinOuterAdditionalDatasets) {
        this.ixJoinOuterAdditionalDatasets = ixJoinOuterAdditionalDatasets;
    }

    public List<ARecordType> getIxJoinOuterAdditionalRecordTypes() {
        return ixJoinOuterAdditionalRecordTypes;
    }

    public void setIxJoinOuterAdditionalRecordTypes(List<ARecordType> ixJoinOuterAdditionalRecordTypes) {
        this.ixJoinOuterAdditionalRecordTypes = ixJoinOuterAdditionalRecordTypes;
    }

    public Map<LogicalVariable, List<String>> getVarsTofieldNameMap() {
        return varsTofieldNameMap;
    }

    /**
     * Sets the field names for variables from the given index-search.
     */
    private void setVarToFieldNameMappingFromUnnestMap(AccessMethodJobGenParams jobGenParams,
            List<LogicalVariable> unnestMapVariables, MetadataProvider metadataProvider) throws AlgebricksException {
        if (jobGenParams != null) {
            // Fetches the associated index.
            Index idxUsedInUnnestMap = metadataProvider.getIndex(jobGenParams.getDataverseName(),
                    jobGenParams.getDatasetName(), jobGenParams.getIndexName());

            if (idxUsedInUnnestMap != null) {
                List<List<String>> idxUsedInUnnestMapFieldNames = idxUsedInUnnestMap.getKeyFieldNames();

                switch (idxUsedInUnnestMap.getIndexType()) {
                    case BTREE:
                        for (int i = 0; i < idxUsedInUnnestMapFieldNames.size(); i++) {
                            varsTofieldNameMap.put(unnestMapVariables.get(i), idxUsedInUnnestMapFieldNames.get(i));
                        }
                        break;
                    // Except the BTree, field name can't be matched directly with variable(s).
                    case RTREE:
                    case SINGLE_PARTITION_NGRAM_INVIX:
                    case SINGLE_PARTITION_WORD_INVIX:
                    case LENGTH_PARTITIONED_NGRAM_INVIX:
                    case LENGTH_PARTITIONED_WORD_INVIX:
                    default:
                        break;
                }
            }
        }
    }

}
