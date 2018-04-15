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
package org.apache.asterix.experiment.builder.stats.suite;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.asterix.experiment.action.base.IAction;
import org.apache.asterix.experiment.action.derived.RunAQLAction;
import org.apache.asterix.experiment.builder.cluster.ICluster8Builder;
import org.apache.asterix.experiment.builder.config.INoStatsBuilder;
import org.apache.asterix.experiment.builder.stats.WorldCupExperimentBuilder;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hyracks.http.server.utils.HttpUtil;

public class WorldCupExperimentNoStats extends WorldCupExperimentBuilder
        implements ICluster8Builder, INoStatsBuilder {

    public WorldCupExperimentNoStats(LSMExperimentSetRunnerConfig config, CloseableHttpClient httpClient) {
        super(config, httpClient);
    }

    @Override
    public IAction getDataDumpAction(OutputStream outputStream, String fieldName) {
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
}
