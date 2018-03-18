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
package org.apache.asterix.experiment.builder;

import static org.apache.asterix.experiment.client.LSMExperimentConstants.BASE_TYPES;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.experiment.action.base.ActionList;
import org.apache.asterix.experiment.action.base.ParallelActionSet;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.AbstractRemoteExecutableAction;
import org.apache.asterix.experiment.action.derived.ForceFlushDatasetAction;
import org.apache.asterix.experiment.action.derived.LogAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.CreateAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.DeleteAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.StopAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.RunAQLAction;
import org.apache.asterix.experiment.action.derived.RunAQLFileAction;
import org.apache.asterix.experiment.action.derived.SleepAction;
import org.apache.asterix.experiment.action.derived.TimedAction;
import org.apache.asterix.experiment.builder.cluster.IClusterBuilder;
import org.apache.asterix.experiment.builder.counter.ICounterBuilder;
import org.apache.asterix.experiment.builder.dgen.IDgenBuilder;
import org.apache.asterix.experiment.builder.experiment.IExperimentBuilder;
import org.apache.asterix.experiment.builder.ingest.IIngestFeedsBuilder;
import org.apache.asterix.experiment.builder.ingest.IIngestMergePolicy;
import org.apache.asterix.experiment.client.GeneratorFactory;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.impl.client.CloseableHttpClient;

public abstract class BaseExperimentBuilder extends AbstractExperimentBuilder implements IExperimentBuilder,
        IClusterBuilder, IDgenBuilder, ICounterBuilder, IIngestFeedsBuilder, IIngestMergePolicy {

    protected static final String ASTERIX_INSTANCE_NAME = "a1";

    protected final CloseableHttpClient httpClient;

    protected final String restHost;

    protected final int restPort;

    protected final String managixHomePath;

    protected final String javaHomePath;

    protected final String username;

    protected final String sshKeyLocation;

    protected final int duration;

    protected String clusterConfigFileName;

    protected String ingestFileName;

    protected String dgenFileName;

    protected String countFileName;

    protected final String statFile;

    protected final String resultsFile;

    protected final SequentialActionList lsAction;

    protected final Path localExperimentRoot;

    protected final int nQueryRuns;

    protected final Path logDir;

    protected final LSMExperimentSetRunnerConfig config;

    protected final String queryOutput;

    protected String asterixConfigFileName;

    protected Set<String> ncHosts;

    protected Cluster cluster;

    protected Map<String, List<String>> dgenPairs;

    protected LSMExperimentSetRunnerConfig.WorkloadType workloadType;

    protected String experimentDDL;

    protected String mergePolicy;

    public BaseExperimentBuilder(LSMExperimentSetRunnerConfig config, CloseableHttpClient httpClient) {
        this.config = config;
        this.restHost = config.getRESTHost();
        this.restPort = config.getRESTPort();
        this.managixHomePath = config.getManagixHome();
        this.javaHomePath = config.getJavaHome();
        this.username = config.getUsername();
        this.sshKeyLocation = config.getSSHKeyLocation();
        this.duration = config.getDatagenDuration();
        this.nQueryRuns = config.getExpRunsNum();
        this.httpClient = httpClient;
        this.clusterConfigFileName = getClusterConfig();
        this.experimentDDL = getExperimentDDL();
        this.mergePolicy = getMergePolicy(config.getComponentsNum());
        this.workloadType = config.getWorkloadType();
        if (config.getIngestType() == LSMExperimentSetRunnerConfig.IngestionType.SocketFeed) {
            this.dgenFileName = getSocketDgen();
        } else {
            this.dgenFileName = getFileDgen();
        }
        this.countFileName = getCounter();
        this.statFile = config.getStatFile();
        this.resultsFile = config.getResultsFile();
        this.queryOutput = config.getQgenOutputFilePath();
        this.lsAction = new SequentialActionList("lsComponents");
        this.localExperimentRoot = Paths.get(config.getLocalExperimentRoot());
        this.logDir = localExperimentRoot.resolve(config.getOutputDir())
                .resolve(LSMExperimentConstants.LOG_DIR + "-" + config.getLogDirSuffix()).resolve(getName());
        this.asterixConfigFileName = LSMExperimentConstants.ASTERIX_DEFAULT_CONFIGURATION;
        this.ncHosts = new HashSet<>();
    }

    protected void doPost(ActionList execs) throws IOException {
    }

    protected abstract ActionList loadData(ActionList dgenActions) throws IOException;

    protected ActionList doBuildDataGen() throws IOException {
        ActionList dgenSeq = new SequentialActionList("dataGen");
        ParallelActionSet dgenActions = new ParallelActionSet("dgen");
        int partition = 0;
        String dgenNodes = "";
        for (Map.Entry<String, List<String>> dgenPair : dgenPairs.entrySet()) {
            if (partition == 0)
                dgenNodes = dgenPair.getKey();
            else
                dgenNodes += "," + dgenPair.getKey();
            final int p = partition;
            dgenActions.addLast(
                    new AbstractRemoteExecutableAction("launch dgens", dgenPair.getKey(), username, sshKeyLocation) {
                @Override
                protected String getCommand() {
                    return GeneratorFactory.getDatagenCmd(config, p,
                            (int) dgenPairs.values().stream().flatMap(c -> c.stream()).count(),
                            String.join(" ", dgenPair.getValue()));
                }
            });
            partition += dgenPair.getValue().size();
        }
        dgenSeq.addLast(new LogAction("Starting data generation on nodes " + dgenNodes));
        dgenSeq.addLast(new TimedAction(dgenActions, (Long time) -> "Data generation took " + time + "ms"));
        return dgenSeq;
    }

    private void setupCluster(ActionList experimentActions, String clusterConfigPath, String asterixConfigPath) {
        //Precondition: create new cluster instance
        experimentActions.addFirst(new CreateAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME,
                clusterConfigPath, asterixConfigPath));
        experimentActions.addFirst(new SleepAction(1000));
        experimentActions.addFirst(new DeleteAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        experimentActions.addFirst(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));

        //Postcondition: stop cluster instance and kill all remaining processes
        experimentActions.addLast(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        ParallelActionSet killCmds = new ParallelActionSet("killCmds");
        for (String ncHost : ncHosts) {
            killCmds.addLast(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {
                @Override
                protected String getCommand() {
                    return localExperimentRoot.resolve("killdrivers.sh").toString();
                }
            });
        }
        killCmds.addLast(new AbstractRemoteExecutableAction(restHost, username, sshKeyLocation) {
            @Override
            protected String getCommand() {
                return localExperimentRoot.resolve("killdrivers.sh").toString();
            }
        });
        experimentActions.addLast(killCmds);
    }

    private void measureIO(ActionList experimentActions) {
        if (statFile != null) {
            ParallelActionSet ioCountActions = new ParallelActionSet("ioCount");
            ParallelActionSet ioCountKillActions = new ParallelActionSet("ioCountKill");
            ParallelActionSet collectIOActions = new ParallelActionSet("collectIO");
            for (String ncHost : ncHosts) {
                ioCountActions.addLast(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {

                    @Override
                    protected String getCommand() {
                        String cmd = "screen -d -m sh -c \"sar -b -u 1 >" + statFile + "\"";
                        return cmd;
                    }
                });
                ioCountKillActions.addLast(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {

                    @Override
                    protected String getCommand() {
                        String cmd = "screen -list | grep Detached | awk '{print $1}' | xargs -I % screen -X -S % quit";
                        return cmd;
                    }
                });
                collectIOActions.addLast(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {

                    @Override
                    protected String getCommand() {
                        String cmd = "cp " + statFile + " " + cluster.getLogDir();
                        return cmd;
                    }
                });
            }
            experimentActions.addFirst(ioCountActions);
            experimentActions.addLast(ioCountKillActions);
            experimentActions.addLast(collectIOActions);
        }
    }

    protected void listIngestedData(ActionList experimentActions) throws IOException {
        SequentialActionList postLSAction = new SequentialActionList("list ingested data action");
        String[] storageRoots = cluster.getIodevices().split(",");
        for (String ncHost : ncHosts) {
            for (final String sRoot : storageRoots) {
                lsAction.addLast(
                        new AbstractRemoteExecutableAction("list components", ncHost, username, sshKeyLocation) {
                    @Override
                    protected String getCommand() {
                        return new StringBuilder().append("ls -la ").append(sRoot).append(File.separator)
                                .append(cluster.getStore()).append(File.separator).append("storage")
                                .append(File.separator).append("partition_*").append(File.separator)
                                .append("experiments").append(File.separator).append("**").append(File.separator)
                                .append("*_b").toString();
                        //                        return new StringBuilder().append("find ").append(sRoot).append(File.separator)
                        //                                .append(cluster.getStore()).append(File.separator).append("storage")
                        //                                .append(File.separator).append("partition_*").append(File.separator)
                        //                                .append("experiments")
                        //                                .append(" -type d | sort | while read -r dir; do n=$(find \"$dir\" -type f -name \"*_b\" | wc -l); printf \"%4d : %s\\n\" $n \"$dir\"; done")
                        //                                .toString();
                    }
                });
                postLSAction.addLast(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {
                    @Override
                    protected String getCommand() {
                        return "du -cksh " + sRoot + "/" + cluster.getStore();
                    }
                });

            }
        }
        experimentActions.addLast(lsAction);
        experimentActions.addLast(postLSAction);
    }

    protected void ingestData(ActionList experimentActions) throws IOException {
        //        experimentActions.addLast(new RunAQLFileAction(httpClient, restHost, restPort,
        //                localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve(LSMExperimentConstants.INGEST_DIR)
        //                        .resolve(ingestFileName)));

        dgenPairs =
                readDatagenPairs(localExperimentRoot.resolve(LSMExperimentConstants.DGEN_DIR).resolve(dgenFileName));
        List<Pair<String, String>> flatDgenPairs = dgenPairs.entrySet().stream()
                .flatMap(c -> c.getValue().stream().map(x -> Pair.of(c.getKey(), x))).collect(Collectors.toList());
        for (Pair<String, String> dgenPair : flatDgenPairs) {
            if (config.getIngestType() == LSMExperimentSetRunnerConfig.IngestionType.SocketFeed)
                ncHosts.add(dgenPair.getRight().split(":")[0]);
            else
                ncHosts.add(dgenPair.getLeft());
        }

        // start record generator
        ActionList dgenActions = doBuildDataGen();

        ActionList ingestActions = loadData(dgenActions);

        //force generated data to be flushed of the disk
        ingestActions
                .addLast(new ForceFlushDatasetAction("experiments", getDatasetName(), httpClient, restHost, restPort));
        ingestActions.addLast(new LogAction(() -> "Flushing dataset " + getDatasetName()));

        //verify records in ingested dataset
        verifyIngestedData(ingestActions);

        experimentActions.addLast(ingestActions);
    }

    protected void verifyIngestedData(ActionList experimentActions) throws IOException {
        experimentActions.addLast(new SleepAction(2000));
        if (countFileName != null) {
            final OutputStream countResultStream = new ByteArrayOutputStream();
            final OutputStream trimmedResultStream = new RunAQLAction.NoNewLineFileOutputStream(countResultStream);
            experimentActions.addLast(new RunAQLFileAction(httpClient, restHost, restPort,
                    localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve(countFileName),
                    trimmedResultStream));
            experimentActions
                    .addLast(new LogAction(() -> "Number of loaded records: " + countResultStream, Level.INFO));
        }

    }

    protected Map<String, List<String>> readDatagenPairs(Path p) throws IOException {
        Map<String, List<String>> dgenPairs = new HashMap<>();
        Scanner s = new Scanner(p, StandardCharsets.UTF_8.name());
        try {
            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] pair = line.split("\\s+");
                List<String> dgenReceivers = dgenPairs.get(pair[0]);
                if (dgenReceivers == null) {
                    dgenReceivers = new ArrayList<>();
                }
                dgenReceivers.add(pair[1]);
                dgenPairs.put(pair[0], dgenReceivers);
            }
        } finally {
            s.close();
        }
        return dgenPairs;
    }

    @Override
    protected void doBuild(Experiment e) throws Exception {
        //some setup
        SequentialActionList execs = new SequentialActionList("main experiment action");

        String clusterConfigPath = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(clusterConfigFileName).toString();
        String asterixConfigPath = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(LSMExperimentConstants.ASTERIX_CONFIGURATION_DIR).resolve(asterixConfigFileName).toString();

        File file = new File(clusterConfigPath);
        JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        cluster = (Cluster) unmarshaller.unmarshal(file);

        //applying experiment actions last to first
        assembleExperiment(execs);
        measureIO(execs);
        setupCluster(execs, clusterConfigPath, asterixConfigPath);

        e.addBody(execs);
    }

    protected void createTypes(ActionList execs) throws IOException {
        execs.addLast(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot
                .resolve(LSMExperimentConstants.AQL_DIR).resolve(workloadType.getDir()).resolve(BASE_TYPES)));
    }

    protected void createDataset(ActionList execs) throws IOException {
        execs.addLast(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot
                .resolve(LSMExperimentConstants.AQL_DIR).resolve(workloadType.getDir()).resolve(experimentDDL)));
    }

    protected void createIndexes(ActionList execs) throws IOException {
    }

    protected void queryData(ActionList execs) throws IOException {
    }

    protected void cleanupData(ActionList execs) throws IOException {
        execs.addLast(new SleepAction(1000));
        //clean up dataset
        execs.addLast(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot
                .resolve(LSMExperimentConstants.AQL_DIR).resolve(LSMExperimentConstants.BASE_CLEANUP)));
    }

    protected void assembleExperiment(ActionList execs) throws Exception {
        execs.addLast(new SleepAction(2000));
        //run ddl statements
        createTypes(execs);
        createDataset(execs);
        createIndexes(execs);

        ingestData(execs);
        execs.addLast(new SleepAction(2000));
        queryData(execs);
        listIngestedData(execs);

        doPost(execs);
        cleanupData(execs);
    }
}
