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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.ActionList;
import org.apache.asterix.experiment.action.base.ParallelActionSet;
import org.apache.asterix.experiment.action.derived.AbstractRemoteExecutableAction;
import org.apache.asterix.experiment.action.derived.LogAction;
import org.apache.asterix.experiment.action.derived.RunAQLAction;
import org.apache.asterix.experiment.action.derived.RunAQLFileAction;
import org.apache.asterix.experiment.action.derived.TimedAction;
import org.apache.asterix.experiment.builder.counter.ITweetRecordsCounterBuilder;
import org.apache.asterix.experiment.client.GeneratorFactory;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;
import org.apache.asterix.experiment.client.LSMStatsExperimentSetRunnerConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hyracks.http.server.utils.HttpUtil;

public abstract class AbstractStatsQueryExperimentBuilder extends AbstractStatsExperimentBuilder
        implements ITweetRecordsCounterBuilder {

    private static final Logger LOGGER = Logger.getLogger(AbstractStatsQueryExperimentBuilder.class.getName());

    protected List<String> qgenHosts;

    public AbstractStatsQueryExperimentBuilder(LSMExperimentSetRunnerConfig config, CloseableHttpClient httpClient) {
        super(config, httpClient);
        LSMStatsExperimentSetRunnerConfig statsConfig = (LSMStatsExperimentSetRunnerConfig) config;
        this.qgenHosts = new ArrayList<>();
    }

    protected void doPost(ActionList execs) throws IOException {
        super.doPost(execs);
        //collect generated query output
        ParallelActionSet collectQueriesActions = new ParallelActionSet("collectQueries");
        for (String qgenHost : qgenHosts) {
            collectQueriesActions.addLast(new AbstractRemoteExecutableAction(qgenHost, username, sshKeyLocation) {
                final String qhostResultsFile = qgenHost + "_" + Paths.get(queryOutput).getFileName().toString();

                @Override
                protected String getCommand() {
                    String cmd = "scp " + username + "@" + qgenHost + ":" + queryOutput + " "
                            + localExperimentRoot.resolve(config.getOutputDir())
                                    .resolve(LSMExperimentConstants.LOG_DIR + "-" + logDirSuffix).resolve(getName())
                                    .resolve(qhostResultsFile).toString();
                    return cmd;
                }
            });
        }
        execs.addLast(collectQueriesActions);
    }

    @Override
    protected void queryData(ActionList seq) throws IOException {
        qgenHosts.addAll(dgenPairs.keySet());

        seq.addLast(new LogAction("Starting query generation on nodes " + StringUtils.join(qgenHosts, ",")));
        ParallelActionSet qgenActions = new ParallelActionSet("qgen");
        int partition = 0;
        for (String qgenHost : qgenHosts) {
            final int p = partition;
            qgenActions.addLast(new AbstractRemoteExecutableAction(qgenHost, username, sshKeyLocation) {
                @Override
                protected String getCommand() {
                    return GeneratorFactory.getQueryGenCmd(config, p, qgenHosts.size());
                }
            });
            partition++;
        }
        seq.addLast(new TimedAction(qgenActions, (Long time) -> "Query generation took " + time + "ms"));
    }

    public String getSynopsisDump() {
        return "dump_synopsis.aql";
    }

    public RunAQLAction getDataDumpAction(OutputStream outputStream) {
        return new RunAQLFileAction(httpClient, restHost, restPort,
                localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve("dump_data.aql"), outputStream,
                HttpUtil.ContentType.CSV);
    }

    @Override
    protected void listIngestedData(ActionList experimentActions) throws IOException {
        super.listIngestedData(experimentActions);
        OutputStream synopsis_os = new FileOutputStream(
                localExperimentRoot.resolve(config.getOutputDir()).resolve(getName() + "_synopsis.csv").toString(),
                false);
        experimentActions.addLast(new RunAQLFileAction(httpClient, restHost, restPort,
                localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve(getSynopsisDump()), synopsis_os,
                HttpUtil.ContentType.CSV));
        OutputStream data_os = new FileOutputStream(
                localExperimentRoot.resolve(config.getOutputDir()).resolve(getName() + "_data.csv").toString(), false);
        experimentActions.addLast(getDataDumpAction(data_os));
    }

}
