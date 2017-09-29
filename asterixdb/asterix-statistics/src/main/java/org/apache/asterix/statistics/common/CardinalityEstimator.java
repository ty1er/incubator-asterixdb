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
package org.apache.asterix.statistics.common;

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Statistics;
import org.apache.asterix.om.base.IAIntegerObject;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.optimizer.rules.am.BTreeSearchArgument;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.CardinalityInferenceVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.ICardinalityEstimator;
import org.apache.hyracks.storage.am.common.api.IIndexSearchArgument;

public class CardinalityEstimator implements ICardinalityEstimator {

    public static CardinalityEstimator INSTANCE = new CardinalityEstimator();

    private long estimationTime;

    private CardinalityEstimator() {
    }

    @Override public long getSelectivity(IIndexSearchArgument searchArg, IMetadataProvider metadataProvider,
            String dataverseName, String datasetName, String indexName) throws AlgebricksException {
        if (!(searchArg instanceof BTreeSearchArgument)) {
            //estimation works only for B-Trees
            return CardinalityInferenceVisitor.UNKNOWN;
        }
        BTreeSearchArgument btreeSearchArg = (BTreeSearchArgument) searchArg;
        if (btreeSearchArg.getNumSecondaryKeys() > 1) {
            //estimation works only for non-composite indexes
            return CardinalityInferenceVisitor.UNKNOWN;
        }
        long startTime = System.nanoTime();
        List<Statistics> stats =
                ((MetadataProvider) metadataProvider).getIndexStatistics(dataverseName, datasetName, indexName);
        double estimate = 0.0;
        for (Statistics s : stats) {
            try {
                double synopsisEstimate = 0.0;
                //point query estimation
                if (btreeSearchArg.isEqCondition()) {
                    //assumes selection with a constant value for point queries
                    long point = extractConstantIntegerExpr(btreeSearchArg.getHighKeyExprs()[0]).longValue();

                    synopsisEstimate = s.getSynopsis().pointQuery(point);
                } else {
                    IAIntegerObject lowKey = extractConstantIntegerExpr(btreeSearchArg.getLowKeyExprs()[0]);
                    IAIntegerObject highKey = extractConstantIntegerExpr(btreeSearchArg.getHighKeyExprs()[0]);
                    long lowKeyValue;
                    int lowKeyAdjustment = 0;
                    if (lowKey == null) {
                        lowKeyValue = highKey.minDomainValue();
                    } else {
                        lowKeyValue = lowKey.longValue();
                        lowKeyAdjustment = btreeSearchArg.getLowKeyInclusive()[0] ? 0 : 1;
                    }
                    long highKeyValue;
                    int highKeyAdjustment = 0;
                    if (highKey == null) {
                        highKeyValue = lowKey.maxDomainValue();
                    } else {
                        highKeyValue = highKey.longValue();
                        highKeyAdjustment = btreeSearchArg.getHighKeyInclusive()[0] ? 0 : 1;
                    }
                    //check validity of range argument
                    if (lowKeyValue < highKeyValue) {
                        synopsisEstimate = s.getSynopsis()
                                .rangeQuery(lowKeyValue + lowKeyAdjustment, highKeyValue - highKeyAdjustment);
                    } else if (lowKeyValue + lowKeyAdjustment == highKeyValue - highKeyAdjustment) {
                        synopsisEstimate = s.getSynopsis().pointQuery(lowKeyValue + lowKeyAdjustment);
                    }
                }
                estimate += synopsisEstimate * (s.isAntimatter() ? -1 : 1);
            } catch (AsterixException e) {
                throw new AlgebricksException(e);
            }
        }
        long endTime = System.nanoTime();
        estimationTime = endTime - startTime;
        if (estimate < 0) {
            return 0L;
        }
        return Math.round(estimate);
    }

    @Override
    public long getEstimationTime() {
        return estimationTime;
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

}
