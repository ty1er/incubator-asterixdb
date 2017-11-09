/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.asterix.experiment.builder.stats.suite;

import org.apache.asterix.experiment.builder.config.IUniformHistBuilder;
import org.apache.asterix.experiment.builder.dgen.IDgen1Builder;
import org.apache.asterix.experiment.builder.ingest.IIngestFeeds1Builder;
import org.apache.asterix.experiment.builder.stats.StatisticsExperiment1Builder;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;
import org.apache.http.impl.client.CloseableHttpClient;

public class StatisticsExperiment1Dgen1Ingest1UniformHist extends StatisticsExperiment1Builder
        implements IDgen1Builder, IIngestFeeds1Builder, IUniformHistBuilder {

    public StatisticsExperiment1Dgen1Ingest1UniformHist(LSMExperimentSetRunnerConfig config,
            CloseableHttpClient httpClient) {
        super(config, httpClient);
    }

}
