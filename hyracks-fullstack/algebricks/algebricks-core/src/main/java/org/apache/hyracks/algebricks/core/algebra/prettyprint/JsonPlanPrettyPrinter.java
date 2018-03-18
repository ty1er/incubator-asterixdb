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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonPlanPrettyPrinter {
    public static void printPlan(ILogicalPlan plan, String planAttributeName, ObjectNode planJson,
            JSONLogicalOperatorPrettyPrintVisitor pvisitor) throws AlgebricksException {
        ObjectMapper om = new ObjectMapper();
        ArrayNode planOpertatorJsonArray = om.createArrayNode();
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printOperator((AbstractLogicalOperator) root.getValue(), pvisitor, planOpertatorJsonArray, om);
        }
        planJson.set(planAttributeName, planOpertatorJsonArray);
    }

    private static void printOperator(AbstractLogicalOperator op, JSONLogicalOperatorPrettyPrintVisitor pvisitor,
            ArrayNode opertatorArray, ObjectMapper om) throws AlgebricksException {
        ObjectNode operatorNode = om.createObjectNode();
        op.accept(pvisitor, operatorNode);
        operatorNode.put("cardinality", op.getCardinality());
        operatorNode.put("executionMode", op.getExecutionMode().toString());

        opertatorArray.add(operatorNode);
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            printOperator((AbstractLogicalOperator) i.getValue(), pvisitor, opertatorArray, om);
        }
    }
}
