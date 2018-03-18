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

import static org.apache.asterix.experiment.client.StatisticsQueryGenerator.getIntType;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.RunPlanAQLStringAction;
import org.apache.asterix.experiment.client.numgen.DistributionType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.impl.client.CloseableHttpClient;

public class WorldCupQueryGenerator extends QueryGenerator {

    private long[] upperBounds;
    private long[] lowerBounds;
    private RangeGenerator[] fieldRangeGens;
    private double rangePercent;
    public static String[] fieldNames =
            { "timestamp", "clientID", "objectID", "size", "status", "server" };

    public WorldCupQueryGenerator(Semaphore sem, WorldCupQueryGeneratorConfig config, int threadsNum,
            FileOutputStream outputFos, CloseableHttpClient httpClient) {
        super(sem, config, threadsNum, outputFos, httpClient);
        this.upperBounds = config.getUpperBounds();
        this.lowerBounds = config.getLowerBounds();
        this.rangePercent = config.getRangePercent();
        this.fieldRangeGens = new RangeGenerator[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++)
            fieldRangeGens[i] =
                    new RangeGenerator.PercentageRangeGenerator(rangePercent, DistributionType.Uniform, upperBounds[i],
                            lowerBounds[i], 0, config.getSeed());
    }

    @Override
    protected void sendQuery() throws IOException {
        SequentialActionList seq = new SequentialActionList("rangeQueryWorldCup");
        ByteArrayOutputStream tmpBuffer = new ByteArrayOutputStream();

        for (int i = 0; i < fieldNames.length; i++) {
            Pair<Long, Long> range = fieldRangeGens[i].getNextRange();
            seq.addLast(new RunPlanAQLStringAction(httpClient, restHost, restPort,
                    getQueryAQL(range, upperBounds[i], lowerBounds[i], fieldNames[i]), tmpBuffer));
            seq.addLast(() -> {
                try {
                    tmpBuffer.write(("," + range.getLeft() + "," + range.getRight() + ",").getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        seq.perform();
        tmpBuffer.write("\n".getBytes());
        outputFos.write(tmpBuffer.toByteArray());
    }

    private String getQueryAQL(Pair<Long, Long> range, long upperBound, long lowerBound, String fieldName) {
        StringBuilder sb = new StringBuilder();
        sb.append("use dataverse experiments; ");
        sb.append("count( ");
        sb.append("for $x in dataset WorldCup").append(" ");
        sb.append("where $x.").append(fieldName).append(" >= ").append(getIntType(upperBound, lowerBound)).append("(\"")
                .append(range.getLeft()).append("\") and $x.").append(fieldName).append(" <= ")
                .append(getIntType(upperBound, lowerBound)).append("(\"").append(range.getRight()).append("\") ");
        sb.append("return $x.").append(fieldName);
        sb.append(");");
        return sb.toString();
    }
}
