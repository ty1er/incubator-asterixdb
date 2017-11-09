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

import static org.apache.asterix.experiment.client.LSMExperimentConstants.SOCKET_FEED;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.asterix.experiment.action.base.AbstractAction;
import org.apache.asterix.experiment.action.base.ActionList;
import org.apache.asterix.experiment.action.base.IAction;
import org.apache.asterix.experiment.action.base.ParallelActionSet;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.ForceFlushDatasetAction;
import org.apache.asterix.experiment.action.derived.LogAction;
import org.apache.asterix.experiment.action.derived.RunAQLFileAction;
import org.apache.asterix.experiment.action.derived.RunAQLStringAction;
import org.apache.asterix.experiment.action.derived.SleepAction;
import org.apache.asterix.experiment.action.derived.TimedAction;
import org.apache.asterix.experiment.builder.BaseExperimentBuilder;
import org.apache.asterix.experiment.builder.Experiment;
import org.apache.asterix.experiment.builder.config.ISynopsisTypeBuilder;
import org.apache.asterix.experiment.builder.ingest.IIngestFeedsBuilder;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;
import org.apache.asterix.experiment.client.LSMStatsExperimentSetRunnerConfig;
import org.apache.asterix.experiment.client.OrchestratorServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.impl.client.CloseableHttpClient;

public class AbstractStatsExperimentBuilder extends BaseExperimentBuilder implements ISynopsisTypeBuilder {

    protected final int ingestFeedsNumber;

    public AbstractStatsExperimentBuilder(LSMExperimentSetRunnerConfig config, CloseableHttpClient httpClient) {
        super(config, httpClient);
        this.ingestFeedsNumber = getIngestFeedsNumber();
        this.asterixConfigFileName = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(LSMExperimentConstants.ASTERIX_DEFAULT_CONFIGURATION).toString();
    }

    @Override
    protected void doBuild(Experiment e) throws Exception {
        LSMStatsExperimentSetRunnerConfig statsConfig = (LSMStatsExperimentSetRunnerConfig) config;
        int synopsisSize = statsConfig.getSynopsisSize();
        File tempAsterixConfig =
                File.createTempFile(LSMExperimentConstants.ASTERIX_DEFAULT_CONFIGURATION, ".tmp." + synopsisSize);
        String asterixConfigTemplate =
                FileUtils.readFileToString(new File(asterixConfigFileName), Charset.defaultCharset());
        String newAsterixConfig = Pattern
                .compile(SYNOPSIS_TYPE_SUBSTITUTE_MARKER).matcher(Pattern.compile(SYNOPSIS_SIZE_SUBSTITUTE_MARKER)
                        .matcher(asterixConfigTemplate).replaceFirst(Integer.toString(synopsisSize)))
                .replaceFirst(getSynopsisType().toString());
        FileUtils.writeStringToFile(tempAsterixConfig, newAsterixConfig, Charset.defaultCharset());
        this.asterixConfigFileName = tempAsterixConfig.getAbsolutePath();

        super.doBuild(e);
    }

    protected String assembleLoad(Path loadTemplate, List<Pair<String, String>> receivers) throws IOException {
        //form a ','-separated list of strings of a form 'host'://'absolute_file_path'
        String files = String.join(",",
                receivers.stream().map(x -> x.getLeft() + "://" + x.getRight()).collect(Collectors.toList()));
        String loadAQL = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(loadTemplate))).toString();
        loadAQL = Pattern.compile(LSMExperimentConstants.LOAD_PATH).matcher(loadAQL)
                .replaceFirst(String.join(",", files));
        return loadAQL;
    }

    protected String assemblingFeedPolicy(String feedAql) throws IOException {
        return Pattern.compile("DISCARD_POLICY").matcher(feedAql).replaceAll(localExperimentRoot
                .resolve(LSMExperimentConstants.CONFIG_DIR).resolve("discard_feed_policy.properties").toString());
    }

    protected String assemblingFeedPolicy(Path feedAqlPath) throws IOException {
        String ingestAql = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(feedAqlPath))).toString();
        return assemblingFeedPolicy(ingestAql);
    }

    protected String assemblingFeedIngest(String ingestAql, int i) throws IOException {
        return Pattern.compile(IIngestFeedsBuilder.INGEST_FEED_NAME).matcher(ingestAql)
                .replaceAll(IIngestFeedsBuilder.INGEST_FEED_NAME + i);
    }

    protected String assemblingFeedIngest(Path ingestAqlPath, int i) throws IOException {
        String ingestAql = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(ingestAqlPath))).toString();
        return assemblingFeedIngest(ingestAql, i);
    }

    protected String setWaitForCompletionIngest(String ingestAql, boolean wait) throws IOException {
        return Pattern.compile(LSMExperimentConstants.WAIT_FOR_COMPLETION).matcher(ingestAql)
                .replaceAll(Boolean.toString(wait));
    }

    protected String setWaitForCompletionIngest(Path ingestAqlPath, boolean wait) throws IOException {
        String ingestAql = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(ingestAqlPath))).toString();
        return setWaitForCompletionIngest(ingestAql, wait);
    }

    protected String assemblingSocketIngest(Path ingestAqlFile, String socketList) throws IOException {
        String ingestAql = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(ingestAqlFile))).toString();
        ingestAql = Pattern.compile(IIngestFeedsBuilder.INGEST_SUBSTITUTE_MARKER).matcher(ingestAql)
                .replaceFirst(socketList);
        return ingestAql;
    }

    @Override
    protected void createDataset(ActionList execs) throws IOException {
        Path ddl = localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve(workloadType.getDir())
                .resolve(experimentDDL);
        String ddlString = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(ddl))).toString();
        long recordNum = (long) (config.getDatagenCount()
                * (1 - 2 * ((LSMStatsExperimentSetRunnerConfig) config).getDeletesPercentage()
                        - ((LSMStatsExperimentSetRunnerConfig) config).getUpdatesPercentage()));
        ddlString = Pattern.compile(LSMExperimentConstants.LOAD_CARDINALITY).matcher(ddlString)
                .replaceFirst(Long.toString(recordNum));
        ddlString = Pattern.compile(LSMExperimentConstants.INGEST_MERGE_POLICY).matcher(ddlString)
                .replaceFirst(mergePolicy);
        execs.addLast(new RunAQLStringAction(httpClient, restHost, restPort, ddlString));
    }

    @Override
    protected void createIndexes(ActionList execs) throws IOException {
        execs.addLast(new RunAQLFileAction(httpClient, restHost, restPort,
                localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve(getIndexDDL())));
    }

    protected List<Pair<String, String>> flattenDgens() {
        List<Pair<String, String>> flatList = new ArrayList<>();
        List<Pair<String, Iterator<String>>> its = new ArrayList<>(dgenPairs.size());
        dgenPairs.forEach((key, value) -> its.add(Pair.of(key, value.iterator())));
        List<Pair<String, Iterator<String>>> activeIterators =
                its.stream().filter(x -> x.getRight().hasNext()).collect(Collectors.toList());
        while (activeIterators.size() > 0) {
            activeIterators.forEach(x -> flatList.add(Pair.of(x.getLeft(), x.getRight().next())));
            activeIterators = its.stream().filter(x -> x.getRight().hasNext()).collect(Collectors.toList());
        }
        return flatList;
    }

    protected void addOrchestrator(ActionList seq, IAction orchestratorStartAction, IAction orchestratorStopAction)
            throws IOException {
        int nIntervals = config.getNIntervals();
        SequentialActionList[] batchActions = new SequentialActionList[nIntervals];
        for (int i = 0; i < nIntervals; i++) {
            batchActions[i] = new SequentialActionList("NewBatchGeneratedOrchestratorAction");
            batchActions[i].addLast(new SleepAction(1000));
            if (i < nIntervals - 1)
                batchActions[i].addLast(new LogAction("Force flush after dataGen interval " + (i + 1)));
            batchActions[i]
                    .addLast(new ForceFlushDatasetAction("experiments", "Tweets", httpClient, restHost, restPort));
            batchActions[i].addLast(new SleepAction(1000));
        }
        final OrchestratorServer oServer = new OrchestratorServer(config.getOrchestratorPort(),
                (int) dgenPairs.values().stream().flatMap(c -> c.stream()).count(), nIntervals, orchestratorStartAction,
                batchActions, orchestratorStopAction);
        seq.addFirst(new AbstractAction("Orchestrator start") {
            @Override
            protected void doPerform() throws Exception {
                oServer.start();
            }
        });
        seq.addLast(new AbstractAction("Orchestrator await") {
            @Override
            protected void doPerform() throws Exception {
                oServer.awaitFinished();
            }
        });
    }

    @Override
    protected ActionList loadData(ActionList dgenActions) throws IOException {
        ActionList loadActions = new SequentialActionList();
        List<Pair<String, String>> flatReceiverList = flattenDgens();
        if (config.getIngestType() == LSMExperimentSetRunnerConfig.IngestionType.SocketFeed
                || config.getIngestType() == LSMExperimentSetRunnerConfig.IngestionType.FileFeed) {
            ActionList runFeedActions = new ParallelActionSet("feedCreate");
            ActionList runConnectActions = new ParallelActionSet("feedConnect");
            ActionList runStartActions = new ParallelActionSet("feedStart");
            ActionList runDisconnectActions = new ParallelActionSet("feedDisconnect");
            int receiversPerFeed = flatReceiverList.size() / ingestFeedsNumber;
            for (int i = 0; i < ingestFeedsNumber; i++) {
                String ingestAql;
                List<Pair<String, String>> receivers = flatReceiverList.subList(i * receiversPerFeed,
                        Math.min((i + 1) * receiversPerFeed, flatReceiverList.size()));
                if (config.getIngestType() == LSMExperimentSetRunnerConfig.IngestionType.SocketFeed) {
                    //receivers are list of sockets, i.e. just right values in dgenPairs
                    String socketReceivers =
                            String.join(",", receivers.stream().map(Pair::getRight).collect(Collectors.toList()));
                    ingestAql = assemblingSocketIngest(localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                            .resolve(config.getWorkloadType().getDir()).resolve(SOCKET_FEED), socketReceivers);
                    runDisconnectActions.addLast(new RunAQLStringAction(httpClient, restHost, restPort,
                            assemblingFeedIngest(localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                                    .resolve(LSMExperimentConstants.INGEST_CLEANUP), i + 1)));
                } else {
                    ingestAql = assembleLoad(localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                            .resolve(config.getWorkloadType().getDir()).resolve(LSMExperimentConstants.FILE_FEED),
                            receivers);
                }
                runFeedActions.addLast(
                        new RunAQLStringAction(httpClient, restHost, restPort, assemblingFeedIngest(ingestAql, i + 1)));
                runConnectActions
                        .addFirst(
                                new RunAQLStringAction(httpClient, restHost, restPort,
                                        assemblingFeedPolicy(
                                                assemblingFeedIngest(
                                                        localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                                                                .resolve(LSMExperimentConstants.INGEST_CONNECT),
                                                        i + 1))));
                runStartActions.addLast(new RunAQLStringAction(httpClient, restHost, restPort,
                        setWaitForCompletionIngest(
                                assemblingFeedIngest(localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                                        .resolve(LSMExperimentConstants.INGEST_START), i + 1),
                                config.getIngestType() != LSMExperimentSetRunnerConfig.IngestionType.SocketFeed)));
            }
            if (config.getIngestType() == LSMExperimentSetRunnerConfig.IngestionType.SocketFeed) {
                final Semaphore dgenSem = new Semaphore(0);
                loadActions.addLast(new AbstractAction() {
                    protected void doPerform() throws Exception {
                        dgenSem.acquire();
                    }
                });
                loadActions.addFirst(dgenActions);
                addOrchestrator(loadActions, () -> {
                    System.out.println("Dgen start action");
                }, dgenSem::release);
                loadActions.addFirst(runStartActions);
                loadActions.addFirst(runConnectActions);
                loadActions.addFirst(runFeedActions);
            } else {
                loadActions.addLast(dgenActions);
                loadActions.addLast(runFeedActions);
                loadActions.addLast(runConnectActions);
                loadActions.addLast(
                        new TimedAction(runStartActions, (Long time) -> "File feed ingestion took " + time + "ms"));
            }
            loadActions.addLast(runDisconnectActions);
        } else if (config.getIngestType() == LSMExperimentSetRunnerConfig.IngestionType.FileLoad) {
            if (workloadType == LSMExperimentSetRunnerConfig.WorkloadType.Varying)
                throw new IOException("Cannot do data load with non-insert data workload");
            loadActions.addLast(dgenActions);
            loadActions.addLast(new TimedAction(
                    new RunAQLStringAction(httpClient, restHost, restPort,
                            assembleLoad(localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                                    .resolve(LSMExperimentConstants.BASE_LOAD), flatReceiverList)),
                    (Long time) -> "Data load took " + time + "ms"));
        }
        return loadActions;
    }
}
