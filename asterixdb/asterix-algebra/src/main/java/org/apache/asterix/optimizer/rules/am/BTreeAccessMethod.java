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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.am.BTreeSearchArgument.LimitType;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.IndexedNLJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

/**
 * Class for helping rewrite rules to choose and apply BTree indexes.
 */
public class BTreeAccessMethod implements IAccessMethod {

    private static final List<FunctionIdentifier> FUNC_IDENTIFIERS =
            Collections.unmodifiableList(Arrays.asList(
                    AlgebricksBuiltinFunctions.EQ,
                    AlgebricksBuiltinFunctions.LE,
                    AlgebricksBuiltinFunctions.GE,
                    AlgebricksBuiltinFunctions.LT,
                    AlgebricksBuiltinFunctions.GT));

    public static final BTreeAccessMethod INSTANCE = new BTreeAccessMethod();

    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
        return FUNC_IDENTIFIERS;
    }

    @Override
    public boolean analyzeFuncExprArgsAndUpdateAnalysisCtx(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        boolean matches =
                AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx(
                        funcExpr, analysisCtx, context, typeEnvironment);
        if (!matches) {
            matches = AccessMethodUtils.analyzeFuncExprArgsForTwoVarsAndUpdateAnalysisCtx(funcExpr, analysisCtx);
        }
        return matches;
    }

    @Override
    public boolean matchAllIndexExprs() {
        return false;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return true;
    }

    @Override
    public boolean applySelectPlanTransformation(List<Mutable<ILogicalOperator>> afterSelectRefs,
            Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException {
        SelectOperator select = (SelectOperator) selectRef.getValue();
        Mutable<ILogicalExpression> conditionRef = select.getCondition();

        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(conditionRef, subTree, null, chosenIndex,
                analysisCtx,
                AccessMethodUtils.retainInputs(subTree.getDataSourceVariables(), subTree.getDataSourceRef().getValue(),
                        afterSelectRefs),
                false, subTree.getDataSourceRef().getValue().getInputs().get(0).getValue()
                        .getExecutionMode() == ExecutionMode.UNPARTITIONED,
                context);

        if (primaryIndexUnnestOp == null) {
            return false;
        }
        Mutable<ILogicalOperator> opRef =
                subTree.getAssignsAndUnnestsRefs().isEmpty() ? null : subTree.getAssignsAndUnnestsRefs().get(0);
        ILogicalOperator op = null;
        if (opRef != null) {
            op = opRef.getValue();
        }
        // Generate new select using the new condition.
        if (conditionRef.getValue() != null) {
            select.getInputs().clear();
            if (op != null) {
                subTree.getDataSourceRef().setValue(primaryIndexUnnestOp);
                select.getInputs().add(new MutableObject<>(op));
            } else {
                select.getInputs().add(new MutableObject<>(primaryIndexUnnestOp));
            }
        } else {
            ((AbstractLogicalOperator) primaryIndexUnnestOp).setExecutionMode(ExecutionMode.PARTITIONED);
            if (op != null) {
                subTree.getDataSourceRef().setValue(primaryIndexUnnestOp);
                selectRef.setValue(op);
            } else {
                selectRef.setValue(primaryIndexUnnestOp);
            }
        }
        return true;
    }

    @Override
    public boolean applyJoinPlanTransformation(Mutable<ILogicalOperator> joinRef,
            OptimizableOperatorSubTree leftSubTree, OptimizableOperatorSubTree rightSubTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context, boolean isLeftOuterJoin,
            boolean hasGroupBy) throws AlgebricksException {
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinRef.getValue();
        Mutable<ILogicalExpression> conditionRef = joinOp.getCondition();
        // Determine if the index is applicable on the left or right side
        // (if both, we arbitrarily prefer the left side).
        Dataset dataset = analysisCtx.getDatasetFromIndexDatasetMap(chosenIndex);
        OptimizableOperatorSubTree indexSubTree;
        OptimizableOperatorSubTree probeSubTree;
        // We assume that the left subtree is the outer branch and the right subtree is the inner branch.
        // This assumption holds true since we only use an index from the right subtree.
        // The following is just a sanity check.
        if (rightSubTree.hasDataSourceScan()
                && dataset.getDatasetName().equals(rightSubTree.getDataset().getDatasetName())) {
            indexSubTree = rightSubTree;
            probeSubTree = leftSubTree;
        } else {
            return false;
        }

        LogicalVariable newNullPlaceHolderVar = null;
        if (isLeftOuterJoin) {
            //get a new null place holder variable that is the first field variable of the primary key
            //from the indexSubTree's datasourceScanOp
            newNullPlaceHolderVar = indexSubTree.getDataSourceVariables().get(0);
        }

        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(conditionRef, indexSubTree, probeSubTree,
                chosenIndex, analysisCtx, true, isLeftOuterJoin, true, context);
        if (primaryIndexUnnestOp == null) {
            return false;
        }

        if (isLeftOuterJoin && hasGroupBy) {
            //reset the null place holder variable
            AccessMethodUtils.resetLOJNullPlaceholderVariableInGroupByOp(analysisCtx, newNullPlaceHolderVar, context);
        }

        // If there are conditions left, add a new select operator on top.
        indexSubTree.getDataSourceRef().setValue(primaryIndexUnnestOp);
        if (conditionRef.getValue() != null) {
            SelectOperator topSelect = new SelectOperator(conditionRef, isLeftOuterJoin, newNullPlaceHolderVar);
            topSelect.getInputs().add(indexSubTree.getRootRef());
            topSelect.setExecutionMode(ExecutionMode.LOCAL);
            context.computeAndSetTypeEnvironmentForOperator(topSelect);
            // Replace the original join with the new subtree rooted at the select op.
            joinRef.setValue(topSelect);
        } else {
            joinRef.setValue(indexSubTree.getRootRef().getValue());
        }
        return true;
    }

    @Override
    public ILogicalOperator createSecondaryToPrimaryPlan(Mutable<ILogicalExpression> conditionRef,
            OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, boolean retainInput, boolean retainNull, boolean requiresBroadcast,
            IOptimizationContext context) throws AlgebricksException {
        Dataset dataset = indexSubTree.getDataset();
        ARecordType recordType = indexSubTree.getRecordType();
        ARecordType metaRecordType = indexSubTree.getMetaRecordType();
        // we made sure indexSubTree has datasource scan
        AbstractDataSourceOperator dataSourceOp = (AbstractDataSourceOperator) indexSubTree.getDataSourceRef().
                getValue();
        int numSecondaryKeys = analysisCtx.getNumberOfMatchedKeys(chosenIndex);
        // List of function expressions that will be replaced by the secondary-index search.
        // These func exprs will be removed from the select condition at the very end of this method.
        Set<ILogicalExpression> replacedFuncExprs = new HashSet<>();

        BTreeSearchArgument searchArg = new BTreeSearchArgument(numSecondaryKeys);
        if (searchArg.createSearchArgument(indexSubTree, probeSubTree, chosenIndex, analysisCtx, context,
                replacedFuncExprs)) {
            return null;
        }
        long selectivity = context.getCardinalityEstimator().getSelectivity(searchArg, context.getMetadataProvider(),
                chosenIndex.getDataverseName(), chosenIndex.getDatasetName(), chosenIndex.getIndexName(),
                String.join(".", chosenIndex.getKeyFieldNames().get(0)));

        // If the select condition contains mixed open/closed intervals on multiple keys, then we make all intervals
        // closed to obtain a superset of answers and leave the original selection in place.
        boolean primaryIndexPostProccessingIsNeeded = false;
        for (int i = 1; i < numSecondaryKeys; ++i) {
            if (searchArg.getLowKeyInclusive()[i] != searchArg.getLowKeyInclusive()[0]) {
                Arrays.fill(searchArg.getLowKeyInclusive(), true);
                primaryIndexPostProccessingIsNeeded = true;
                break;
            }
        }
        for (int i = 1; i < numSecondaryKeys; ++i) {
            if (searchArg.getHighKeyInclusive()[i] != searchArg.getHighKeyInclusive()[0]) {
                Arrays.fill(searchArg.getHighKeyInclusive(), true);
                primaryIndexPostProccessingIsNeeded = true;
                break;
            }
        }

        if (searchArg.getLowKeyLimits()[0] == null) {
            searchArg.getLowKeyInclusive()[0] = true;
        }
        if (searchArg.getHighKeyLimits()[0] == null) {
            searchArg.getHighKeyInclusive()[0] = true;
        }

        // determine cases when prefix search could be applied
        for (int i = 1; i < searchArg.getLowKeyExprs().length; i++) {
            if (searchArg.getLowKeyLimits()[0] == null && searchArg.getLowKeyLimits()[i] != null
                    || searchArg.getLowKeyLimits()[0] != null && searchArg.getLowKeyLimits()[i] == null
                    || searchArg.getHighKeyLimits()[0] == null && searchArg.getHighKeyLimits()[i] != null
                    || searchArg.getHighKeyLimits()[0] != null && searchArg.getHighKeyLimits()[i] == null) {
                numSecondaryKeys--;
                primaryIndexPostProccessingIsNeeded = true;
            }
        }

        // Here we generate vars and funcs for assigning the secondary-index keys to be fed into the secondary-index
        // search.
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<>();
        // List of variables and expressions for the assign.
        ArrayList<LogicalVariable> assignKeyVarList = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> assignKeyExprList = new ArrayList<>();
        int numLowKeys =
                createKeyVarsAndExprs(numSecondaryKeys, searchArg.getLowKeyLimits(), searchArg.getLowKeyExprs(),
                        assignKeyVarList, assignKeyExprList, keyVarList, context,
                        searchArg.getConstantAtRuntimeExpressions(), searchArg.getConstAtRuntimeExprVars());
        int numHighKeys =
                createKeyVarsAndExprs(numSecondaryKeys, searchArg.getHighKeyLimits(), searchArg.getHighKeyExprs(),
                        assignKeyVarList, assignKeyExprList, keyVarList, context,
                        searchArg.getConstantAtRuntimeExpressions(), searchArg.getConstAtRuntimeExprVars());

        BTreeJobGenParams jobGenParams = new BTreeJobGenParams(chosenIndex.getIndexName(), IndexType.BTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, requiresBroadcast);
        jobGenParams.setLowKeyInclusive(searchArg.getLowKeyInclusive()[0]);
        jobGenParams.setHighKeyInclusive(searchArg.getHighKeyInclusive()[0]);
        jobGenParams.setIsEqCondition(searchArg.isEqCondition());
        jobGenParams.setLowKeyVarList(keyVarList, 0, numLowKeys);
        jobGenParams.setHighKeyVarList(keyVarList, numLowKeys, numHighKeys);

        ILogicalOperator inputOp;
        if (!assignKeyVarList.isEmpty()) {
            // Assign operator that sets the constant secondary-index search-key fields if necessary.
            AssignOperator assignSearchKeys = new AssignOperator(assignKeyVarList, assignKeyExprList);
            if (probeSubTree == null) {
                // We are optimizing a selection query.
                // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
                assignSearchKeys.getInputs().add(new MutableObject<>(
                        OperatorManipulationUtil.deepCopy(dataSourceOp.getInputs().get(0).getValue())));
                assignSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
            } else {
                // We are optimizing a join, place the assign op top of the probe subtree.
                assignSearchKeys.getInputs().add(probeSubTree.getRootRef());
            }
            inputOp = assignSearchKeys;
        } else if (probeSubTree == null) {
            //nonpure case
            //Make sure that the nonpure function is unpartitioned
            ILogicalOperator checkOp = dataSourceOp.getInputs().get(0).getValue();
            while (checkOp.getExecutionMode() != ExecutionMode.UNPARTITIONED) {
                if (checkOp.getInputs().size() == 1) {
                    checkOp = checkOp.getInputs().get(0).getValue();
                } else {
                    return null;
                }
            }
            inputOp = dataSourceOp.getInputs().get(0).getValue();
        } else {
            // All index search keys are variables.
            inputOp = probeSubTree.getRoot();
        }

        ILogicalOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                metaRecordType, chosenIndex, inputOp, jobGenParams, context, false, retainInput, retainNull);

        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        AbstractUnnestMapOperator primaryIndexUnnestOp = null;

        boolean isPrimaryIndex = chosenIndex.isPrimaryIndex();
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            // External dataset
            UnnestMapOperator externalDataAccessOp = AccessMethodUtils.createExternalDataLookupUnnestMap(dataSourceOp,
                    dataset, recordType, secondaryIndexUnnestOp, context, retainInput, retainNull);
            indexSubTree.getDataSourceRef().setValue(externalDataAccessOp);
            primaryIndexUnnestOp = externalDataAccessOp;
        } else if (!isPrimaryIndex) {
            primaryIndexUnnestOp = AccessMethodUtils.createPrimaryIndexUnnestMap(dataSourceOp, dataset, recordType,
                    metaRecordType, secondaryIndexUnnestOp, context, true, retainInput, retainNull, false);

            // Adds equivalence classes --- one equivalent class between a primary key
            // variable and a record field-access expression.
            EquivalenceClassUtils.addEquivalenceClassesForPrimaryIndexAccess(primaryIndexUnnestOp,
                    dataSourceOp.getVariables(), recordType, metaRecordType, dataset, context);
        } else {
            List<Object> primaryIndexOutputTypes = new ArrayList<>();
            AccessMethodUtils.appendPrimaryIndexTypes(dataset, recordType, metaRecordType, primaryIndexOutputTypes);
            List<LogicalVariable> scanVariables = dataSourceOp.getVariables();

            // Checks whether the primary index search can replace the given
            // SELECT condition.
            // If so, condition will be set to null and eventually the SELECT
            // operator will be removed.
            // If not, we create a new condition based on remaining ones.
            if (!primaryIndexPostProccessingIsNeeded) {
                List<Mutable<ILogicalExpression>> remainingFuncExprs = new ArrayList<>();
                try {
                    getNewConditionExprs(conditionRef, replacedFuncExprs, remainingFuncExprs);
                } catch (CompilationException e) {
                    return null;
                }
                // Generate new condition.
                if (!remainingFuncExprs.isEmpty()) {
                    ILogicalExpression pulledCond = createSelectCondition(remainingFuncExprs);
                    conditionRef.setValue(pulledCond);
                } else {
                    conditionRef.setValue(null);
                }
            }

            // Checks whether LEFT_OUTER_UNNESTMAP operator is required.
            boolean leftOuterUnnestMapRequired = false;
            if (retainNull && retainInput) {
                leftOuterUnnestMapRequired = true;
            } else {
                leftOuterUnnestMapRequired = false;
            }

            if (conditionRef.getValue() != null) {
                // The job gen parameters are transferred to the actual job gen
                // via the UnnestMapOperator's function arguments.
                List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<>();
                jobGenParams.writeToFuncArgs(primaryIndexFuncArgs);
                // An index search is expressed as an unnest-map over an
                // index-search function.
                IFunctionInfo primaryIndexSearch = FunctionUtil.getFunctionInfo(BuiltinFunctions.INDEX_SEARCH);
                UnnestingFunctionCallExpression primaryIndexSearchFunc =
                        new UnnestingFunctionCallExpression(primaryIndexSearch, primaryIndexFuncArgs);
                primaryIndexSearchFunc.setReturnsUniqueValues(true);
                if (!leftOuterUnnestMapRequired) {
                    primaryIndexUnnestOp = new UnnestMapOperator(scanVariables,
                            new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes,
                            retainInput);
                } else {
                    primaryIndexUnnestOp = new LeftOuterUnnestMapOperator(scanVariables,
                            new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes,
                            true);
                }
            } else {
                if (!leftOuterUnnestMapRequired) {
                    primaryIndexUnnestOp = new UnnestMapOperator(scanVariables,
                            ((UnnestMapOperator) secondaryIndexUnnestOp).getExpressionRef(), primaryIndexOutputTypes,
                            retainInput);
                } else {
                    primaryIndexUnnestOp = new LeftOuterUnnestMapOperator(scanVariables,
                            ((LeftOuterUnnestMapOperator) secondaryIndexUnnestOp).getExpressionRef(),
                            primaryIndexOutputTypes, true);
                }
            }

            primaryIndexUnnestOp.getInputs().add(new MutableObject<>(inputOp));

            // Adds equivalence classes --- one equivalent class between a primary key
            // variable and a record field-access expression.
            EquivalenceClassUtils.addEquivalenceClassesForPrimaryIndexAccess(primaryIndexUnnestOp, scanVariables,
                    recordType, metaRecordType, dataset, context);
        }

        return primaryIndexUnnestOp;
    }

    private int createKeyVarsAndExprs(int numKeys, LimitType[] keyLimits, ILogicalExpression[] searchKeyExprs,
            ArrayList<LogicalVariable> assignKeyVarList, ArrayList<Mutable<ILogicalExpression>> assignKeyExprList,
            ArrayList<LogicalVariable> keyVarList, IOptimizationContext context, ILogicalExpression[] constExpressions,
            LogicalVariable[] constExprVars) {
        if (keyLimits[0] == null) {
            return 0;
        }
        for (int i = 0; i < numKeys; i++) {
            ILogicalExpression searchKeyExpr = searchKeyExprs[i];
            ILogicalExpression constExpression = constExpressions[i];
            LogicalVariable keyVar = null;
            if (searchKeyExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                keyVar = context.newVar();
                assignKeyExprList.add(new MutableObject<>(searchKeyExpr));
                assignKeyVarList.add(keyVar);
            } else {
                keyVar = ((VariableReferenceExpression) searchKeyExpr).getVariableReference();
                if (constExpression != null) {
                    assignKeyExprList.add(new MutableObject<>(constExpression));
                    assignKeyVarList.add(constExprVars[i]);
                }
            }
            keyVarList.add(keyVar);
        }
        return numKeys;
    }

    private void getNewConditionExprs(Mutable<ILogicalExpression> conditionRef,
            Set<ILogicalExpression> replacedFuncExprs, List<Mutable<ILogicalExpression>> remainingFuncExprs)
            throws CompilationException {
        remainingFuncExprs.clear();
        if (replacedFuncExprs.isEmpty()) {
            return;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) conditionRef.getValue();
        if (replacedFuncExprs.size() == 1) {
            Iterator<ILogicalExpression> it = replacedFuncExprs.iterator();
            if (!it.hasNext()) {
                return;
            }
            if (funcExpr == it.next()) {
                // There are no remaining function exprs.
                return;
            }
        }
        // The original select cond must be an AND. Check it just to be sure.
        if (funcExpr.getFunctionIdentifier() != AlgebricksBuiltinFunctions.AND) {
            throw new CompilationException(ErrorCode.COMPILATION_FUNC_EXPRESSION_CANNOT_UTILIZE_INDEX,
                    funcExpr.toString());
        }
        // Clean the conjuncts.
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            ILogicalExpression argExpr = arg.getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            // If the function expression was not replaced by the new index
            // plan, then add it to the list of remaining function expressions.
            if (!replacedFuncExprs.contains(argExpr)) {
                remainingFuncExprs.add(arg);
            }
        }
    }

    private ILogicalExpression createSelectCondition(List<Mutable<ILogicalExpression>> predList) {
        if (predList.size() > 1) {
            IFunctionInfo finfo = FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.AND);
            return new ScalarFunctionCallExpression(finfo, predList);
        }
        return predList.get(0).getValue();
    }

    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) throws AlgebricksException {
        // If we are optimizing a join, check for the indexed nested-loop join hint.
        if (optFuncExpr.getNumLogicalVars() == 2) {
            if (optFuncExpr.getOperatorSubTree(0) == optFuncExpr.getOperatorSubTree(1)) {
                if ((optFuncExpr.getSourceVar(0) == null && optFuncExpr.getFieldType(0) != null)
                        || (optFuncExpr.getSourceVar(1) == null && optFuncExpr.getFieldType(1) != null)) {
                    //We are in the select case (trees are the same, and one field comes from non-scan)
                    //We can do the index search
                } else {
                    //One of the vars was from an assign rather than a scan
                    //And we were unable to determine its type
                    return false;
                }
            } else if (!optFuncExpr.getFuncExpr().getAnnotations()
                    .containsKey(IndexedNLJoinExpressionAnnotation.INSTANCE)) {
                return false;
            }
        }
        if (!index.isPrimaryIndex() && optFuncExpr.getFuncExpr().getAnnotations()
                .containsKey(SkipSecondaryIndexSearchExpressionAnnotation.INSTANCE)) {
            return false;
        }
        // No additional analysis required for BTrees.
        return true;
    }

    @Override
    public String getName() {
        return "BTREE_ACCESS_METHOD";
    }

    @Override
    public int compareTo(IAccessMethod o) {
        return this.getName().compareTo(o.getName());
    }

}
