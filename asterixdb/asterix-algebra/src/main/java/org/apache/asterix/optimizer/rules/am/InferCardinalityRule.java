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

import static org.apache.asterix.optimizer.rules.am.BTreeAccessMethod.LimitType.EQUAL;

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.IAIntegerObject;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.optimizer.rules.am.BTreeAccessMethod.LimitType;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.CardinalityInferenceVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Uses accumulated statistics to infer cardinality of the select/join operations
 * Matches the following patterns:
 * (select) <-- (assign)? <-- (datasource scan) or
 * (join) <-- (assign)? <--(datasource scan)
 * <-- (assign)? <-- (datasource scan)
 */
public class InferCardinalityRule implements IAlgebraicRewriteRule {

    protected OptimizableOperatorSubTree leftSubTree;
    protected OptimizableOperatorSubTree rightSubTree;

    protected boolean checkAndReturnExpr(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // First check that the operator is a select or join and its condition is a function call.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT || op.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
            leftSubTree = new OptimizableOperatorSubTree();
            leftSubTree.initFromSubTree(op.getInputs().get(0));
            leftSubTree.setDatasetAndTypeMetadata(metadataProvider);
        } else {
            return false;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
            rightSubTree = new OptimizableOperatorSubTree();
            rightSubTree.initFromSubTree(op.getInputs().get(1));
            rightSubTree.setDatasetAndTypeMetadata(metadataProvider);
        }
        return true;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (checkAndReturnExpr(opRef, context)) {
            op.setCardinality(inferCardinality(opRef, context));
            return true;
        } else {
            if (!opRef.getValue().getInputs().isEmpty()) {
                op.setCardinality(opRef.getValue().getInputs().get(0).getValue().getCardinality());
                return true;
            }
        }
        return false;
    }

    private long inferCardinality(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        context.addToDontApplySet(this, op);

        ILogicalExpression condExpr = null;
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            condExpr = ((SelectOperator) op).getCondition().getValue();
        } else if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
            condExpr = ((InnerJoinOperator) op).getCondition().getValue();
        }
        if (condExpr == null || condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return CardinalityInferenceVisitor.UNKNOWN;
        }

        IVariableTypeEnvironment typeEnvironment = context.getOutputTypeEnvironment(op);
        AccessMethodAnalysisContext analysisCtx = analyzeCondition(condExpr, context, typeEnvironment);

        if (analysisCtx.getMatchedFuncExprs().isEmpty()) {
            return CardinalityInferenceVisitor.UNKNOWN;
        }

        for (int j = 0; j < analysisCtx.getMatchedFuncExprs().size(); j++) {
            IOptimizableFuncExpr optFuncExpr = analysisCtx.getMatchedFuncExpr(j);
            fillOptimizableFuncExpr(optFuncExpr, leftSubTree);
            if (rightSubTree != null) {
                fillOptimizableFuncExpr(optFuncExpr, rightSubTree);
            }
        }

        IAIntegerObject lowKey = null;
        IAIntegerObject highKey = null;
        int lowKeyAdjustment = 0;
        int highKeyAdjustment = 0;
        List<String> leftField = null;
        List<String> rightField = null;
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.getMatchedFuncExprs()) {
            OptimizableOperatorSubTree optSubTree = optFuncExpr.getOperatorSubTree(0);
            ILogicalExpression expr = AccessMethodUtils.createSearchKeyExpr(false, optFuncExpr,
                    optFuncExpr.getFieldType(0), optSubTree).first;
            LimitType limit = BTreeAccessMethod.getLimitType(optFuncExpr, optSubTree);
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                //inferring cardinality for join
                if (limit != EQUAL) {
                    // cannot calculate cardinality for non equi-joins
                    return CardinalityInferenceVisitor.UNKNOWN;
                }
                if (leftField != null && rightField != null) {
                    // cannot calculate cardinality for conjunctive join conditions
                    return CardinalityInferenceVisitor.UNKNOWN;
                }
                leftField = optFuncExpr.getFieldName(0);
                rightField = optFuncExpr.getFieldName(1);
            } else if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                if (leftField == null) {
                    leftField = optFuncExpr.getFieldName(0);
                } else if (!leftField.equals(optFuncExpr.getFieldName(0))) {
                    // cannot calculate cardinality for expressions on different fields
                    return CardinalityInferenceVisitor.UNKNOWN;
                }
                //inferring cardinality for selection
                switch (limit) {
                    case EQUAL:
                        highKey = extractConstantIntegerExpr(expr);
                        lowKey = highKey;
                        break;
                    case HIGH_EXCLUSIVE:
                        lowKeyAdjustment = 1;
                    case HIGH_INCLUSIVE:
                        lowKey = extractConstantIntegerExpr(expr);
                        break;
                    case LOW_EXCLUSIVE:
                        highKeyAdjustment = -1;
                    case LOW_INCLUSIVE:
                        highKey = extractConstantIntegerExpr(expr);
                        break;
                }
            }
        }
        if (leftField != null && rightField != null) {
            //estimate join cardinality
            return context.getCardinalityEstimator().getJoinCardinality(context.getMetadataProvider(),
                    leftSubTree.getDataset().getDataverseName(), leftSubTree.getDataset().getDatasetName(), leftField,
                    rightSubTree.getDataset().getDataverseName(), rightSubTree.getDataset().getDatasetName(),
                    rightField);
        } else if (leftField != null || rightField != null) {
            Long lowKeyValue = null;
            Long highKeyValue = null;
            if (lowKey != null) {
                lowKeyValue = lowKey.longValue() + lowKeyAdjustment;
            } else if (highKey != null) {
                lowKeyValue = highKey.minDomainValue();
            }
            if (highKey != null) {
                highKeyValue = highKey.longValue() + highKeyAdjustment;
            } else if (lowKey != null) {
                highKeyValue = lowKey.maxDomainValue();
            }
            if (lowKeyValue != null && highKeyValue != null) {
                return context.getCardinalityEstimator().getRangeCardinality(context.getMetadataProvider(),
                        leftSubTree.getDataset().getDataverseName(), leftSubTree.getDataset().getDatasetName(),
                        leftField, lowKeyValue, highKeyValue);
            }
        }
        return CardinalityInferenceVisitor.UNKNOWN;
    }

    protected AccessMethodAnalysisContext analyzeCondition(ILogicalExpression cond, IOptimizationContext context,
            IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        AccessMethodAnalysisContext analysisCtx = new AccessMethodAnalysisContext();
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) cond;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (funcIdent != AlgebricksBuiltinFunctions.OR) {
            analyzeFunctionExpr(funcExpr, analysisCtx, context, typeEnvironment);
            for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                ILogicalExpression argExpr = arg.getValue();
                if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    continue;
                }
                analyzeFunctionExpr((AbstractFunctionCallExpression) argExpr, analysisCtx, context, typeEnvironment);
            }
        }
        return analysisCtx;
    }

    private void analyzeFunctionExpr(AbstractFunctionCallExpression funcExpr, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (funcIdent == AlgebricksBuiltinFunctions.LE || funcIdent == AlgebricksBuiltinFunctions.GE
                || funcIdent == AlgebricksBuiltinFunctions.LT || funcIdent == AlgebricksBuiltinFunctions.GT
                || funcIdent == AlgebricksBuiltinFunctions.EQ) {
            boolean matches = AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx(funcExpr,
                    analysisCtx, context, typeEnvironment);
            if (!matches) {
                AccessMethodUtils.analyzeFuncExprArgsForTwoVarsAndUpdateAnalysisCtx(funcExpr, analysisCtx);
            }
        }
    }

    private IAIntegerObject extractConstantIntegerExpr(ILogicalExpression expr) throws AsterixException {
        if (expr == null) {
            return null;
        }
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            IAObject constExprValue = ((AsterixConstantValue) ((ConstantExpression) expr).getValue()).getObject();
            if (ATypeHierarchy.belongsToDomain(constExprValue.getType().getTypeTag(), ATypeHierarchy.Domain.INTEGER)) {
                return (IAIntegerObject) constExprValue;
            }
        }
        return null;

    }

    private List<String> fillOptimizableFuncExpr(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree subTree)
            throws AlgebricksException {
        for (AbstractLogicalOperator subTreeOp : subTree.getAssignsAndUnnests()) {
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                List<LogicalVariable> varList = ((AssignOperator) subTreeOp).getVariables();
                for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                    LogicalVariable var = varList.get(varIndex);
                    int funcVarIndex = optFuncExpr.findLogicalVar(var);
                    if (funcVarIndex == -1) {
                        continue;
                    }
                    Pair<ARecordType, List<String>> fieldNameTypePair = IntroduceLSMComponentFilterRule
                            .getFieldNameFromSubAssignTree(optFuncExpr, subTreeOp, varIndex, subTree.getRecordType());
                    if (fieldNameTypePair == null) {
                        return null;
                    }
                    optFuncExpr.setFieldName(funcVarIndex, fieldNameTypePair.second);
                    optFuncExpr.setFieldType(funcVarIndex,
                            fieldNameTypePair.first.getSubFieldType(fieldNameTypePair.second));
                    optFuncExpr.setOptimizableSubTree(funcVarIndex, subTree);
                    return fieldNameTypePair.second;
                }
            }
        }
        // Try to match variables from optFuncExpr to datasourcescan if not
        // already matched in assigns.
        List<LogicalVariable> dsVarList = subTree.getDataSourceVariables();

        for (int varIndex = 0; varIndex < dsVarList.size(); varIndex++) {
            LogicalVariable var = dsVarList.get(varIndex);
            int funcVarIndex = optFuncExpr.findLogicalVar(var);
            // No matching var in optFuncExpr.
            if (funcVarIndex == -1) {
                continue;
            }

            // The variable value is one of the partitioning fields.
            List<String> fieldName = null;
            List<List<String>> subTreePKs = null;
            subTreePKs = subTree.getDataset().getPrimaryKeys();
            // Check whether this variable is PK, not a record variable.
            if (varIndex <= subTreePKs.size() - 1) {
                fieldName = subTreePKs.get(varIndex);

            }
            optFuncExpr.setFieldName(funcVarIndex, fieldName);
            optFuncExpr.setOptimizableSubTree(funcVarIndex, subTree);
            optFuncExpr.setSourceVar(funcVarIndex, var);
            optFuncExpr.setFieldType(funcVarIndex, subTree.getRecordType().getSubFieldType(fieldName));
            optFuncExpr.setLogicalExpr(funcVarIndex, new VariableReferenceExpression(var));
            return fieldName;
        }

        return null;
    }
}
