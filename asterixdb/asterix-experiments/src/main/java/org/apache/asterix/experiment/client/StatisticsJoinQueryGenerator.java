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
package org.apache.asterix.experiment.client;

import static org.apache.asterix.experiment.client.DataGeneratorForSpatialIndexEvaluation.NUM_BTREE_EXTRA_FIELDS;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.RunPlanAQLStringAction;
import org.apache.http.impl.client.CloseableHttpClient;

public class StatisticsJoinQueryGenerator extends StatisticsRangeQueryGenerator {

    public StatisticsJoinQueryGenerator(Semaphore sem, StatisticsRangeQueryGeneratorConfig config, int threadsNum,
            FileOutputStream outputFos, CloseableHttpClient httpClient) {
        super(sem, config, threadsNum, outputFos, httpClient);
    }

    protected void sendQuery() throws IOException {
        SequentialActionList seq = new SequentialActionList("joinQuery");
        //create action
        ByteArrayOutputStream tmpBuffer = new ByteArrayOutputStream();

        //perform
        seq.perform();
        tmpBuffer.write("\n".getBytes());

        for (int i = 1; i <= NUM_BTREE_EXTRA_FIELDS; i++) {
            for (int j = 1; j <= NUM_BTREE_EXTRA_FIELDS; j++) {
                seq.addLast(new RunPlanAQLStringAction(httpClient, restHost, restPort,
                        joinQueryAQL("btree-extra-field" + i, "btree-extra-field" + j), tmpBuffer));
            }

        }

        int permits = 0;
        try {
            permits = sem.drainPermits();
            outputFos.write(tmpBuffer.toByteArray());
        } finally {
            sem.release(permits);
        }
    }

    private String joinQueryAQL(String field1, String field2) {
        StringBuilder sb = new StringBuilder();
        sb.append("use dataverse experiments;\n");
        sb.append("count(\n");
        sb.append("for $x in dataset Tweets\n");
        sb.append("for $y in dataset Tweets\n");
        sb.append("where $x.").append(field1).append("=$y.").append(field2);
        sb.append("return {\"x\":$x, \"y\":$y}\n");
        sb.append(");");
        return sb.toString();
    }
}
