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
package org.apache.hyracks.algebricks.core.algebra.prettyprint;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class JSONLogicalOperatorPrettyPrintVisitor implements ILogicalOperatorVisitor<Void, ObjectNode> {

    private static String OPERATOR_FIELD = "operator";

    @Override
    public Void visitAggregateOperator(AggregateOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "aggregate");
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "running-aggregate");
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "empty-tuple-source");
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "group-by");
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "limit");
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "join");
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "left-outer-join");
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, ObjectNode arg)
            throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "nested-tuple-source");
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "order-by");
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "assign");
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "select");
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "delegate");
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "project");
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "replicate");
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "split");
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "materialize");
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "script");
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "subplan");
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "sink");
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "union");
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "intersect");
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "unnest");
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "left-outer-unnest");
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "unnest-map");
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, ObjectNode arg)
            throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "left-outer-unnest");
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "data-scan");
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "distinct");
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "exchange");
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "write");
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "distribute-result");
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "load");
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, ObjectNode arg)
            throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "insert-delete-upsert");
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, ObjectNode arg)
            throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "index-insert-delete-upsert");
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, ObjectNode arg) throws AlgebricksException {
        arg.put(OPERATOR_FIELD, "tokenize");
        return null;
    }
}
