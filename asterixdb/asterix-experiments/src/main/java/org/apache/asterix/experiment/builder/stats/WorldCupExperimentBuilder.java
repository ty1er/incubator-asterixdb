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
import java.io.StringWriter;
import java.util.List;
import java.util.function.Consumer;

import org.apache.asterix.experiment.action.base.ActionList;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.LogAction;
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

public class WorldCupExperimentBuilder extends AbstractStatsQueryExperimentBuilder
        implements IIngestFeeds1Builder, IPrefixMergePolicy {

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
    public String getDataDump() {
        return "worldcup/dump_data.aql";
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
        return new SequentialActionList();
    }

    @Override
    protected void verifyIngestedData(ActionList experimentActions) throws IOException {
        super.verifyIngestedData(experimentActions);
        experimentActions.addLast(new SleepAction(1000));
        getMinMax(experimentActions, ((LSMStatsExperimentSetRunnerConfig) config)::setLowerBound, "min");
        getMinMax(experimentActions, ((LSMStatsExperimentSetRunnerConfig) config)::setUpperBound, "max");
    }

    private void getMinMax(ActionList experimentActions, Consumer<String> f, String func) throws IOException {
        new StringWriter();
        final ByteArrayOutputStream countResultStream = new ByteArrayOutputStream();
        experimentActions
                .addLast(new RunAQLStringAction(httpClient, restHost, restPort, getMinMaxAql(func), countResultStream));
        experimentActions.addLast(() -> f.accept("\"" + new String(countResultStream.toByteArray()) + "\""));
        experimentActions
                .addLast(new LogAction(() -> "Domain " + func + ":" + new String(countResultStream.toByteArray())));
    }

    private String getMinMaxAql(String function) {
        StringBuilder sb = new StringBuilder();
        sb.append("use dataverse experiments;\n");
        for (int i = 0; i < WorldCupQueryGenerator.fieldNames.length; i++) {
            sb.append(function).append("(for $x in dataset WorldCup return $x.")
                    .append(WorldCupQueryGenerator.fieldNames[i]).append(")\n");
        }
        return sb.toString();
    }

    @Override
    protected ActionList loadData(ActionList dgenActions) throws IOException {
        ActionList loadActions = new SequentialActionList();
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
}
