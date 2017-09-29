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

import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.storage.am.common.api.IIndexSearchArgument;

public class BTreeSearchArgument implements IIndexSearchArgument {

    // Describes whether a search predicate is an open/closed interval.
    public enum LimitType {
        LOW_INCLUSIVE, LOW_EXCLUSIVE, HIGH_INCLUSIVE, HIGH_EXCLUSIVE, EQUAL;

        public static LimitType getLimitType(IOptimizableFuncExpr optFuncExpr,
                OptimizableOperatorSubTree probeSubTree) {
            ComparisonKind ck =
                    AlgebricksBuiltinFunctions.getComparisonType(optFuncExpr.getFuncExpr().getFunctionIdentifier());
            LimitType limit = null;
            switch (ck) {
                case EQ: {
                    limit = LimitType.EQUAL;
                    break;
                }
                case GE: {
                    limit = probeIsOnLhs(optFuncExpr, probeSubTree) ?
                            LimitType.HIGH_INCLUSIVE :
                            LimitType.LOW_INCLUSIVE;
                    break;
                }
                case GT: {
                    limit = probeIsOnLhs(optFuncExpr, probeSubTree) ?
                            LimitType.HIGH_EXCLUSIVE :
                            LimitType.LOW_EXCLUSIVE;
                    break;
                }
                case LE: {
                    limit = probeIsOnLhs(optFuncExpr, probeSubTree) ?
                            LimitType.LOW_INCLUSIVE :
                            LimitType.HIGH_INCLUSIVE;
                    break;
                }
                case LT: {
                    limit = probeIsOnLhs(optFuncExpr, probeSubTree) ?
                            LimitType.LOW_EXCLUSIVE :
                            LimitType.HIGH_EXCLUSIVE;
                    break;
                }
                case NEQ: {
                    limit = null;
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
            return limit;
        }

        private static boolean probeIsOnLhs(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree probeSubTree) {
            if (probeSubTree == null) {
                if (optFuncExpr.getConstantExpressions().length == 0) {
                    return optFuncExpr.getLogicalExpr(0) == null;
                }
                // We are optimizing a selection query. Search key is a constant. Return true if constant is on lhs.
                return optFuncExpr.getFuncExpr().getArguments().get(0) == optFuncExpr.getConstantExpr(0);
            } else {
                // We are optimizing a join query. Determine whether the feeding variable is on the lhs.
                return (optFuncExpr.getOperatorSubTree(0) == null || optFuncExpr.getOperatorSubTree(0) == probeSubTree);
            }
        }
    }

    private int numSecondaryKeys;
    private ILogicalExpression[] lowKeyExprs;
    private ILogicalExpression[] highKeyExprs;
    private LimitType[] lowKeyLimits;
    private LimitType[] highKeyLimits;
    private boolean[] lowKeyInclusive;
    private boolean[] highKeyInclusive;
    // Info on high and low keys for the BTree search predicate.
    private ILogicalExpression[] constantAtRuntimeExpressions;
    private LogicalVariable[] constAtRuntimeExprVars;
    private boolean isEqCondition = false;

    public BTreeSearchArgument(int numSecondaryKeys) {
        this.numSecondaryKeys = numSecondaryKeys;
        this.lowKeyExprs = new ILogicalExpression[numSecondaryKeys];
        this.highKeyExprs = new ILogicalExpression[numSecondaryKeys];
        this.lowKeyLimits = new LimitType[numSecondaryKeys];
        this.highKeyLimits = new LimitType[numSecondaryKeys];
        this.lowKeyInclusive = new boolean[numSecondaryKeys];
        this.highKeyInclusive = new boolean[numSecondaryKeys];
        this.constantAtRuntimeExpressions = new ILogicalExpression[numSecondaryKeys];
        this.constAtRuntimeExprVars = new LogicalVariable[numSecondaryKeys];
        this.isEqCondition = false;
    }

    public int getNumSecondaryKeys() {
        return numSecondaryKeys;
    }

    public ILogicalExpression[] getLowKeyExprs() {
        return lowKeyExprs;
    }

    public ILogicalExpression[] getHighKeyExprs() {
        return highKeyExprs;
    }

    public LimitType[] getLowKeyLimits() {
        return lowKeyLimits;
    }

    public LimitType[] getHighKeyLimits() {
        return highKeyLimits;
    }

    public boolean[] getLowKeyInclusive() {
        return lowKeyInclusive;
    }

    public boolean[] getHighKeyInclusive() {
        return highKeyInclusive;
    }

    public ILogicalExpression[] getConstantAtRuntimeExpressions() {
        return constantAtRuntimeExpressions;
    }

    public LogicalVariable[] getConstAtRuntimeExprVars() {
        return constAtRuntimeExprVars;
    }

    public boolean isEqCondition() {
        return isEqCondition;
    }

    public boolean createSearchArgument(OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, Set<ILogicalExpression> replacedFuncExprs) throws AlgebricksException {
        List<Pair<Integer, Integer>> exprAndVarList = analysisCtx.getIndexExprsFromIndexExprsAndVars(chosenIndex);

        /* TODO: For now we don't do any sophisticated analysis of the func exprs to come up with "the best" range
         * predicate. If we can't figure out how to integrate a certain funcExpr into the current predicate,
         * we just bail by setting this flag.*/
        boolean couldntFigureOut = false;
        boolean doneWithExprs = false;
        // TODO: For now don't consider prefix searches.
        BitSet setLowKeys = new BitSet(numSecondaryKeys);
        BitSet setHighKeys = new BitSet(numSecondaryKeys);
        // Go through the func exprs listed as optimizable by the chosen index,
        // and formulate a range predicate on the secondary-index keys.

        for (Pair<Integer, Integer> exprIndex : exprAndVarList) {
            // Position of the field of matchedFuncExprs.get(exprIndex) in the chosen index's indexed exprs.
            IOptimizableFuncExpr optFuncExpr = analysisCtx.getMatchedFuncExpr(exprIndex.first);
            int keyPos = chosenIndex.getKeyFieldNames().indexOf(optFuncExpr.getFieldName(0));
            if (keyPos < 0 && optFuncExpr.getNumLogicalVars() > 1) {
                // If we are optimizing a join, the matching field may be the second field name.
                keyPos = chosenIndex.getKeyFieldNames().indexOf(optFuncExpr.getFieldName(1));
            }
            if (keyPos < 0) {
                throw CompilationException.create(ErrorCode.NO_INDEX_FIELD_NAME_FOR_GIVEN_FUNC_EXPR);
            }
            IAType indexedFieldType = chosenIndex.getKeyFieldTypes().get(keyPos);
            Pair<ILogicalExpression, Boolean> returnedSearchKeyExpr =
                    AccessMethodUtils.createSearchKeyExpr(chosenIndex, optFuncExpr, indexedFieldType, indexSubTree, probeSubTree);
            ILogicalExpression searchKeyExpr = returnedSearchKeyExpr.first;
            if (searchKeyExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                constantAtRuntimeExpressions[keyPos] = searchKeyExpr;
                constAtRuntimeExprVars[keyPos] = context.newVar();
                searchKeyExpr = new VariableReferenceExpression(constAtRuntimeExprVars[keyPos]);

            }

            LimitType limit = LimitType.getLimitType(optFuncExpr, probeSubTree);
            if (limit == null) {
                return false;
            }

            // checks whether a type casting happened from a real (FLOAT, DOUBLE) value to an INT value
            // since we have a round issues when dealing with LT(<) OR GT(>) operator.
            boolean realTypeConvertedToIntegerType = returnedSearchKeyExpr.second;

            if (relaxLimitTypeToInclusive(chosenIndex, keyPos, realTypeConvertedToIntegerType)) {
                if (limit == LimitType.HIGH_EXCLUSIVE) {
                    limit = LimitType.HIGH_INCLUSIVE;
                } else if (limit == LimitType.LOW_EXCLUSIVE) {
                    limit = LimitType.LOW_INCLUSIVE;
                }
            }

            switch (limit) {
                case EQUAL: {
                    if (lowKeyLimits[keyPos] == null && highKeyLimits[keyPos] == null) {
                        lowKeyLimits[keyPos] = highKeyLimits[keyPos] = limit;
                        lowKeyInclusive[keyPos] = highKeyInclusive[keyPos] = true;
                        lowKeyExprs[keyPos] = highKeyExprs[keyPos] = searchKeyExpr;
                        setLowKeys.set(keyPos);
                        setHighKeys.set(keyPos);
                        isEqCondition = true;
                    } else {
                        // Has already been set to the identical values.
                        // When optimizing join we may encounter the same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (lowKeyLimits[keyPos] == limit && lowKeyInclusive[keyPos] == true && lowKeyExprs[keyPos]
                                .equals(searchKeyExpr) && highKeyLimits[keyPos] == limit
                                && highKeyInclusive[keyPos] == true && highKeyExprs[keyPos].equals(searchKeyExpr)) {
                            isEqCondition = true;
                            break;
                        }
                        couldntFigureOut = true;
                    }
                    // TODO: For now don't consider prefix searches.
                    // If high and low keys are set, we exit for now.
                    if (setLowKeys.cardinality() == numSecondaryKeys && setHighKeys.cardinality() == numSecondaryKeys) {
                        doneWithExprs = true;
                    }
                    break;
                }
                case HIGH_EXCLUSIVE: {
                    if (highKeyLimits[keyPos] == null || (highKeyLimits[keyPos] != null && highKeyInclusive[keyPos])) {
                        highKeyLimits[keyPos] = limit;
                        highKeyExprs[keyPos] = searchKeyExpr;
                        highKeyInclusive[keyPos] = false;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the
                        // same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (highKeyLimits[keyPos] == limit && highKeyInclusive[keyPos] == false && highKeyExprs[keyPos]
                                .equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case HIGH_INCLUSIVE: {
                    if (highKeyLimits[keyPos] == null) {
                        highKeyLimits[keyPos] = limit;
                        highKeyExprs[keyPos] = searchKeyExpr;
                        highKeyInclusive[keyPos] = true;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the
                        // same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (highKeyLimits[keyPos] == limit && highKeyInclusive[keyPos] == true && highKeyExprs[keyPos]
                                .equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_EXCLUSIVE: {
                    if (lowKeyLimits[keyPos] == null || (lowKeyLimits[keyPos] != null && lowKeyInclusive[keyPos])) {
                        lowKeyLimits[keyPos] = limit;
                        lowKeyExprs[keyPos] = searchKeyExpr;
                        lowKeyInclusive[keyPos] = false;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the
                        // same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (lowKeyLimits[keyPos] == limit && lowKeyInclusive[keyPos] == false && lowKeyExprs[keyPos]
                                .equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_INCLUSIVE: {
                    if (lowKeyLimits[keyPos] == null) {
                        lowKeyLimits[keyPos] = limit;
                        lowKeyExprs[keyPos] = searchKeyExpr;
                        lowKeyInclusive[keyPos] = true;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the
                        // same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (lowKeyLimits[keyPos] == limit && lowKeyInclusive[keyPos] == true && lowKeyExprs[keyPos]
                                .equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
            if (!couldntFigureOut) {
                // Remember to remove this funcExpr later.
                replacedFuncExprs.add(analysisCtx.getMatchedFuncExpr(exprIndex.first).getFuncExpr());
            }
            if (doneWithExprs) {
                break;
            }
        }
        return couldntFigureOut;
    }

    private static boolean relaxLimitTypeToInclusive(Index chosenIndex, int keyPos, boolean realTypeConvertedToIntegerType) {
        // If a DOUBLE or FLOAT constant is converted to an INT type value,
        // we need to check a corner case where two real values are located between an INT value.
        // For example, for the following query,
        //
        // for $emp in dataset empDataset
        // where $emp.age > double("2.3") and $emp.age < double("3.3")
        // return $emp.id
        //
        // It should generate a result if there is a tuple that satisfies the condition, which is 3,
        // however, it does not generate the desired result since finding candidates
        // fail after truncating the fraction part (there is no INT whose value is greater than 2 and less than 3.)
        //
        // Therefore, we convert LT(<) to LE(<=) and GT(>) to GE(>=) to find candidates.
        // This does not change the result of an actual comparison since this conversion is only applied
        // for finding candidates from an index.
        //
        // We also need to do this for a non-enforced index that overrides key field type (for a numeric type)

        if (realTypeConvertedToIntegerType) {
            return true;
        }

        if (chosenIndex.isOverridingKeyFieldTypes() && !chosenIndex.isEnforced()) {
            IAType indexedKeyType = chosenIndex.getKeyFieldTypes().get(keyPos);
            if (NonTaggedFormatUtil.isOptional(indexedKeyType)) {
                indexedKeyType = ((AUnionType) indexedKeyType).getActualType();
            }
            switch (indexedKeyType.getTypeTag()) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                    return true;
                default:
                    break;
            }
        }

        return false;
    }
}
