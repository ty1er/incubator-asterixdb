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

import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.experiment.action.base.ParallelActionSet;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.*;
import org.apache.asterix.experiment.action.derived.ManagixActions.CreateAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.DeleteAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.LogAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.StopAsterixManagixAction;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;
import org.apache.asterix.experiment.client.LSMPerfConstants;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class is used to create experiments for spatial index static data evaluation, that is, no ingestion is involved.
 * Also, there is no orchestration server involved in this experiment builder.
 */
public abstract class AbstractPerfLoadBuilder extends AbstractExperimentBuilder {

    private static final String ASTERIX_INSTANCE_NAME = "a1";

    private final String logDirSuffix;

    protected final CloseableHttpClient httpClient;

    protected final String restHost;

    protected final int restPort;

    private final String managixHomePath;

    protected final String javaHomePath;

    protected final Path localExperimentRoot;

    protected final String username;

    protected final String sshKeyLocation;

    private final String clusterConfigFileName;

    protected final String dgenFileName;

    private final String countFileName;

    private final String statFile;

    protected final SequentialActionList lsAction;

    protected final String loadAQLFilePath;

    protected final String querySQLPPFileName;

    public AbstractPerfLoadBuilder(String name, LSMExperimentSetRunnerConfig config,
            String clusterConfigFileName, String dgenFileName,
            String countFileName, String loadAQLFileName, String querySQLPPFileName) {
        this.logDirSuffix = config.getLogDirSuffix();
        PoolingHttpClientConnectionManager poolingCCM = new PoolingHttpClientConnectionManager();
        poolingCCM.setDefaultMaxPerRoute(10);
        this.httpClient = HttpClients.custom().setConnectionManager(poolingCCM).setMaxConnPerRoute(10).build();
        this.restHost = config.getRESTHost();
        this.restPort = config.getRESTPort();
        this.managixHomePath = config.getManagixHome();
        this.javaHomePath = config.getJavaHome();
        this.localExperimentRoot = Paths.get(config.getLocalExperimentRoot());
        this.username = config.getUsername();
        this.sshKeyLocation = config.getSSHKeyLocation();
        this.clusterConfigFileName = clusterConfigFileName;
        this.dgenFileName = dgenFileName;
        this.countFileName = countFileName;
        this.statFile = config.getStatFile();
        this.lsAction = new SequentialActionList();
        this.loadAQLFilePath = loadAQLFileName;
        this.querySQLPPFileName = querySQLPPFileName;
    }

    protected abstract void doBuildDDL(SequentialActionList seq) throws IOException;

    @Override
    protected void doBuild(Experiment e) throws IOException, JAXBException {
        SequentialActionList execs = new SequentialActionList();

        String clusterConfigPath = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(clusterConfigFileName).toString();
        String asterixConfigPath = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(LSMExperimentConstants.ASTERIX_DEFAULT_CONFIGURATION).toString();

        //stop/delete/create instance
        execs.addLast(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.addLast(new DeleteAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.addLast(new SleepAction(30000));
        execs.addLast(new CreateAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME, clusterConfigPath,
                asterixConfigPath));

        //ddl statements
        execs.addLast(new SleepAction(15000));
        // TODO: implement retry handler
        execs.addLast(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(LSMPerfConstants.BASE_TYPES)));
        doBuildDDL(execs);

        //prepare io state action in NC node(s)
        Map<String, List<String>> dgenPairs = readDatagenPairs(localExperimentRoot.resolve(
                LSMExperimentConstants.DGEN_DIR).resolve(dgenFileName));
        final Set<String> ncHosts = new HashSet<>();
        for (List<String> ncHostList : dgenPairs.values()) {
            for (String ncHost : ncHostList) {
                ncHosts.add(ncHost.split(":")[0]);
            }
        }
        if (statFile != null) {
            ParallelActionSet ioCountActions = new ParallelActionSet();
            for (String ncHost : ncHosts) {
                ioCountActions.addLast(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {

                    @Override
                    protected String getCommand() {
                        String cmd = "screen -d -m sh -c \"sar -b -u 1 > " + statFile + "\"";
                        return cmd;
                    }
                });
            }
            execs.addLast(ioCountActions);
        }

        //prepare post ls action
        File file = new File(clusterConfigPath);
        JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        final Cluster cluster = (Cluster) unmarshaller.unmarshal(file);
        String[] storageRoots = cluster.getIodevices().split(",");

        //---------- main experiment body begins -----------

        //run DDL + Load
        execs.addLast(new TimedAction(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(loadAQLFilePath))));

        //execute SQL++ Queries
        execs.addLast(new TimedAction(new RunSQLPPFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(querySQLPPFileName),
                localExperimentRoot.resolve(LSMPerfConstants.RESULT_FILE))));

        //---------- main experiment body ends -----------

        //kill io state action
        if (statFile != null) {
            ParallelActionSet ioCountKillActions = new ParallelActionSet();
            for (String ncHost : ncHosts) {
                ioCountKillActions.addLast(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {

                    @Override
                    protected String getCommand() {
                        String cmd = "screen -X -S `screen -list | grep Detached | awk '{print $1}'` quit";
                        return cmd;
                    }
                });
            }
            execs.addLast(ioCountKillActions);
        }

        //total record count
        execs.addLast(new SleepAction(10000));
        if (countFileName != null) {
            execs.addLast(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                    LSMExperimentConstants.AQL_DIR).resolve(countFileName)));
        }

        execs.addLast(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));

        //prepare to collect io state by putting the state file into asterix log dir
        if (statFile != null) {
            ParallelActionSet collectIOActions = new ParallelActionSet();
            for (String ncHost : ncHosts) {
                collectIOActions.addLast(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {

                    @Override
                    protected String getCommand() {
                        String cmd = "cp " + statFile + " " + cluster.getLogDir();
                        return cmd;
                    }
                });
            }
            execs.addLast(collectIOActions);
        }

        //collect cc and nc logs
        execs.addLast(new LogAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME, localExperimentRoot
                .resolve(LSMExperimentConstants.LOG_DIR + "-" + logDirSuffix).resolve(getName()).toString()));

        e.addBody(execs);
    }

    protected Map<String, List<String>> readDatagenPairs(Path p) throws IOException {
        Map<String, List<String>> dgenPairs = new HashMap<>();
        Scanner s = new Scanner(p, StandardCharsets.UTF_8.name());
        try {
            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] pair = line.split("\\s+");
                List<String> vals = dgenPairs.get(pair[0]);
                if (vals == null) {
                    vals = new ArrayList<>();
                    dgenPairs.put(pair[0], vals);
                }
                vals.add(pair[1]);
            }
        } finally {
            s.close();
        }
        return dgenPairs;
    }
}
