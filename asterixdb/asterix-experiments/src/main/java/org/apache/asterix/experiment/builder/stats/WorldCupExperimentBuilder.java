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
package org.apache.asterix.experiment.builder.stats;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.experiment.action.base.ActionList;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.LogAction;
import org.apache.asterix.experiment.action.derived.RunAQLAction;
import org.apache.asterix.experiment.action.derived.RunAQLStringAction;
import org.apache.asterix.experiment.action.derived.SleepAction;
import org.apache.asterix.experiment.action.derived.TimedAction;
import org.apache.asterix.experiment.builder.ingest.IIngestFeeds1Builder;
import org.apache.asterix.experiment.builder.ingest.IPrefixMergePolicy;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig.IngestionType;
import org.apache.asterix.experiment.client.LSMStatsExperimentSetRunnerConfig;
import org.apache.asterix.experiment.client.WorldCupQueryGenerator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hyracks.http.server.utils.HttpUtil;

public class WorldCupExperimentBuilder extends AbstractStatsQueryExperimentBuilder
        implements IIngestFeeds1Builder, IPrefixMergePolicy {

    private List<String> fieldMinimums = new ArrayList<>();
    private List<String> fieldMaximum = new ArrayList<>();

    public WorldCupExperimentBuilder(LSMExperimentSetRunnerConfig config, CloseableHttpClient httpClient) {
        super(config, httpClient);
    }

    @Override
    public String getExperimentDDL() {
        return "dataset.aql";
    }

    @Override
    public String getIndexDDL() {
        return "worldcup/index.aql";
    }

    @Override
    public String getCounter() {
        return "worldcup/count.aql";
    }

    @Override
    public RunAQLAction getDataDumpAction(OutputStream outputStream, String fieldName) {
        return new RunAQLAction(httpClient, restHost, restPort, outputStream, HttpUtil.ContentType.CSV) {

            @Override
            public void doPerform() throws Exception {
                String aql = StandardCharsets.UTF_8
                        .decode(ByteBuffer.wrap(Files.readAllBytes(localExperimentRoot
                                .resolve(LSMExperimentConstants.AQL_DIR).resolve("worldcup/dump_data.aql"))))
                        .toString();
                for (int i = 0; i < getFieldNames().length; i++) {
                    if (getFieldNames()[i].equals(fieldName)) {
                        aql = aql.replaceAll("FIELD_MIN", fieldMinimums.get(i));
                        aql = aql.replaceAll("FIELD_MAX", fieldMaximum.get(i));
                        aql = aql.replaceAll("FIELD", fieldName);
                        performAqlAction(aql);
                        break;
                    }
                }
            }

        };
    }

    @Override
    public String getDatasetName() {
        return "WorldCup";
    }

    @Override
    public String getFileDgen() {
        return "worldcup.dgen";
    }

    @Override
    protected ActionList doBuildDataGen() throws IOException {
        return new SequentialActionList("dataGenWorldCup");
    }

    @Override
    protected void verifyIngestedData(ActionList experimentActions) throws IOException {
        super.verifyIngestedData(experimentActions);
        experimentActions.addLast(new SleepAction(1000));
        getMinMax(experimentActions, (LSMStatsExperimentSetRunnerConfig) config);
    }

    private void getMinMax(ActionList experimentActions, LSMStatsExperimentSetRunnerConfig statisConfig) {

        for (int i = 0; i < WorldCupQueryGenerator.fieldNames.length; i++) {
            final ByteArrayOutputStream resultStream = new ByteArrayOutputStream();
            StringBuilder query = new StringBuilder();
            final String fieldName = WorldCupQueryGenerator.fieldNames[i];
            query.append("use dataverse experiments;\n\n");
            query.append("let $wc := for $x in dataset WorldCup where $x.").append(fieldName)
                    .append(" >= -2147483648 return $x.").append(fieldName).append("\n");
            query.append("return {\"min\": min($wc),\"max\": max($wc)};");

            experimentActions.addLast(new RunAQLStringAction(httpClient, restHost, restPort, query.toString(),
                    resultStream, HttpUtil.ContentType.CSV));
            experimentActions.addLast(() -> {
                for (String s : new String(resultStream.toByteArray()).split("\r\n")) {
                    String[] minMax = s.split(",");
                    fieldMinimums.add(minMax[0]);
                    fieldMaximum.add(minMax[1]);
                }
            });
            experimentActions.addLast(() -> {
                statisConfig.setLowerBound(String.join("\t", fieldMinimums));
                statisConfig.setUpperBound(String.join("\t", fieldMaximum));
            });
            experimentActions.addLast(new LogAction(
                    () -> "Min/max values for field \"" + fieldName + "\":" + new String(resultStream.toByteArray())));
        }

    }

    @Override
    protected ActionList loadData(ActionList dgenActions) throws IOException {
        ActionList loadActions = new SequentialActionList("loadDataWorldCup");
        List<Pair<String, String>> flatReceiverList = flattenDgens();
        if (config.getIngestType() == IngestionType.FileFeed) {
            String ingestAql = assembleLoad(
                    localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                            .resolve(config.getWorkloadType().getDir()).resolve(LSMExperimentConstants.FILE_FEED),
                    flatReceiverList);
            loadActions.addLast(new TimedAction(new RunAQLStringAction(httpClient, restHost, restPort, ingestAql),
                    (Long time) -> "File feed ingestion took " + time + "ms"));
        } else if (config.getIngestType() == IngestionType.FileLoad) {
            loadActions.addLast(new TimedAction(
                    new RunAQLStringAction(httpClient, restHost, restPort,
                            assembleLoad(localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                                    .resolve(LSMExperimentConstants.BASE_LOAD), flatReceiverList)),
                    (Long time) -> "Data load took " + time + "ms"));
        }
        return loadActions;
    }

    @Override
    protected String[] getFieldNames() {
        return WorldCupQueryGenerator.fieldNames;
    }
}
