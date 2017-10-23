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
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.IAction;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.RunPlanAQLStringAction;
import org.apache.asterix.experiment.client.numgen.DistributionType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.impl.client.CloseableHttpClient;

public class StatisticsQueryGenerator extends QueryGenerator {

    private static final Logger LOGGER = Logger.getLogger(QueryGenerator.class.getName());

    private RangeGenerator[] rangeGens;
    private StatisticsRangeType rangeType;
    private long upperBound;
    private long lowerBound;

    public StatisticsQueryGenerator(Semaphore sem, StatisticsQueryGeneratorConfig config, int threadsNum,
            FileOutputStream outputFos, CloseableHttpClient httpClient) {
        super(sem, config, threadsNum, outputFos, httpClient);
        rangeType = config.getRangeType();
        upperBound = config.getUpperBound();
        lowerBound = config.getLowerBound();
        rangeGens = new RangeGenerator[DistributionType.values().length];
        for (int i = 0; i < DistributionType.values().length; i++)
            rangeGens[i] = RangeGenerator.getRangeGenerator(config.getRangeType(), upperBound, lowerBound,
                    config.getRangeLength(), config.getRangePercent(), config.getQueryCount(),
                    DistributionType.values()[i], config.getSkew(), config.getSeed());
    }

    protected void sendQuery() throws IOException {
        SequentialActionList seq = new SequentialActionList();
        //create action
        ByteArrayOutputStream tmpBuffer = new ByteArrayOutputStream();
        for (int i = 1; i <= NUM_BTREE_EXTRA_FIELDS; i++) {
            //prepare range
            Pair<Long, Long> range = rangeGens[(i - 1) / 3].getNextRange();

            IAction rangeQueryAction = new RunPlanAQLStringAction(httpClient, restHost, restPort,
                    getQueryAQL(range, "btree-extra-field" + i), tmpBuffer);
            seq.addLast(rangeQueryAction);
            seq.addLast(new IAction() {
                @Override
                public void perform() {
                    try {
                        tmpBuffer.write(("," + range.getLeft() + "," + range.getRight() + ",").getBytes());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        //perform
        seq.perform();
        tmpBuffer.write("\n".getBytes());

        int permits = 0;
        try {
            permits = sem.drainPermits();
            outputFos.write(tmpBuffer.toByteArray());
        } finally {
            sem.release(permits);
        }
    }

    public static String getIntType(long upperBound, long lowerBound) {
        if (upperBound <= Byte.MAX_VALUE && lowerBound >= Byte.MIN_VALUE)
            return "int8";
        else if (upperBound <= Short.MAX_VALUE && lowerBound >= Short.MIN_VALUE)
            return "int16";
        else if (upperBound <= Integer.MAX_VALUE && lowerBound >= Integer.MIN_VALUE)
            return "int32";
        else
            return "int64";
    }

    private String getQueryAQL(Pair<Long, Long> range, String fieldName) {
        StringBuilder sb = new StringBuilder();
        sb.append("use dataverse experiments; ");
        sb.append("count( ");
        sb.append("for $x in dataset Tweets").append(" ");
        if (rangeType == StatisticsRangeType.CorrelatedPoint || rangeType == StatisticsRangeType.Point) {
            sb.append("where $x.").append(fieldName).append(" = ").append(getIntType(upperBound, lowerBound))
                    .append("(\"").append(range.getLeft()).append("\") ");
        } else {
            sb.append("where $x.").append(fieldName).append(" >= ").append(getIntType(upperBound, lowerBound))
                    .append("(\"").append(range.getLeft()).append("\") and $x.").append(fieldName).append(" <= ")
                    .append(getIntType(upperBound, lowerBound)).append("(\"").append(range.getRight()).append("\") ");
        }
        sb.append("return $x.").append(fieldName);
        sb.append(");");
        return sb.toString();
    }

}
