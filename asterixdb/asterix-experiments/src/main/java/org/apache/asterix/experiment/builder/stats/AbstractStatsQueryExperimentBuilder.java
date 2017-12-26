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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.ActionList;
import org.apache.asterix.experiment.action.derived.AbstractLocalExecutableAction;
import org.apache.asterix.experiment.action.derived.RunAQLAction;
import org.apache.asterix.experiment.action.derived.RunAQLStringAction;
import org.apache.asterix.experiment.action.derived.TimedAction;
import org.apache.asterix.experiment.builder.counter.ITweetRecordsCounterBuilder;
import org.apache.asterix.experiment.client.DataGeneratorForSpatialIndexEvaluation;
import org.apache.asterix.experiment.client.GeneratorFactory;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hyracks.http.server.utils.HttpUtil;

public abstract class AbstractStatsQueryExperimentBuilder extends AbstractStatsExperimentBuilder
        implements ITweetRecordsCounterBuilder {

    private static final Logger LOGGER = Logger.getLogger(AbstractStatsQueryExperimentBuilder.class.getName());

    public AbstractStatsQueryExperimentBuilder(LSMExperimentSetRunnerConfig config, CloseableHttpClient httpClient) {
        super(config, httpClient);
    }

    class LazyFileOutputStream extends OutputStream {
        private final String path;
        private FileOutputStream fos;
        private boolean initialized = false;

        public LazyFileOutputStream(String path) {
            this.path = path;
        }

        @Override
        public void write(int b) throws IOException {
            if (!initialized) {
                fos = new FileOutputStream(path);
                initialized = true;
            }
            fos.write(b);
        }
    }

    @Override
    protected void doPost(ActionList execs) throws IOException {
        super.doPost(execs);
        // dump synopsis and dataset CDF
        for (String fieldName : getFieldNames()) {
            OutputStream synopsis_os =
                    new LazyFileOutputStream(logDir.resolve(getName() + "_" + fieldName + "_synopsis.csv").toString());
            execs.addLast(getSynopsisDumpAction(synopsis_os, fieldName));
            OutputStream data_os =
                    new LazyFileOutputStream(logDir.resolve(getName() + "_" + fieldName + "_data.csv").toString());
            execs.addLast(getDataDumpAction(data_os, fieldName));
        }
        //collect generated query output
        execs.addLast(new AbstractLocalExecutableAction() {
            @Override
            protected String getCommand() {
                return "mv " + queryOutput + " "
                        + localExperimentRoot.resolve(logDir).resolve(Paths.get(queryOutput).getFileName()).toString();
            }
        });
    }

    @Override
    protected void queryData(ActionList seq) throws IOException {
        seq.addLast(new TimedAction(new AbstractLocalExecutableAction() {
            @Override
            protected String getCommand() {
                return GeneratorFactory.getQueryGenCmd(config, 1, 1);
            }

            @Override
            protected Map<String, String> getEnvironment() {
                Map<String, String> envMap = new HashMap<>();
                //envMap.put("JAVA_OPTS", "-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=3000,suspend=y");
                envMap.put("JAVA_HOME", config.getJavaHome());
                return envMap;

            }
        }, (Long time) -> "Query generation took " + time + "ms"));
    }

    public String getSynopsisDump() {
        return "dump_synopsis.aql";
    }

    public RunAQLAction getSynopsisDumpAction(OutputStream outputStream, String fieldName) throws IOException {
        String aql = StandardCharsets.UTF_8
                .decode(ByteBuffer.wrap(Files.readAllBytes(
                        localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve(getSynopsisDump()))))
                .toString();
        aql = aql.replaceAll("FIELD", fieldName);
        return new RunAQLStringAction(httpClient, restHost, restPort, aql, outputStream, HttpUtil.ContentType.CSV);
    }

    public RunAQLAction getDataDumpAction(OutputStream outputStream, String fieldName) throws IOException {
        String aql = StandardCharsets.UTF_8
                .decode(ByteBuffer.wrap(Files.readAllBytes(
                        localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve("dump_data.aql"))))
                .toString();
        aql = aql.replaceAll("FIELD", fieldName);
        return new RunAQLStringAction(httpClient, restHost, restPort, aql, outputStream, HttpUtil.ContentType.CSV);
    }

    protected String[] getFieldNames() {
        String[] result = new String[DataGeneratorForSpatialIndexEvaluation.NUM_BTREE_EXTRA_FIELDS];
        for (int i = 1; i <= DataGeneratorForSpatialIndexEvaluation.NUM_BTREE_EXTRA_FIELDS; i++) {
            result[i - 1] = "btree-extra-field" + i;
        }
        return result;
    }

}
