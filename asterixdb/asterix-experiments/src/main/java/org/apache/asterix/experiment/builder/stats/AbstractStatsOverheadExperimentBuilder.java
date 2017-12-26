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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.experiment.action.base.ActionList;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.LogAction;
import org.apache.asterix.experiment.builder.cluster.ICluster4Partition2Builder;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;
import org.apache.http.impl.client.CloseableHttpClient;

public abstract class AbstractStatsOverheadExperimentBuilder extends AbstractStatsExperimentBuilder
        implements ICluster4Partition2Builder {

    private static final Logger LOGGER = Logger.getLogger(AbstractStatsOverheadExperimentBuilder.class.getName());

    public AbstractStatsOverheadExperimentBuilder(LSMExperimentSetRunnerConfig config, CloseableHttpClient httpClient) {
        super(config, httpClient);
    }

    @Override
    protected void listIngestedData(ActionList experimentActions) throws IOException {
        //no listing of ingested data
    }

    protected void assembleExperiment(ActionList execs) throws Exception {
        for (int runNum = 0; runNum < nQueryRuns; runNum++) {
            execs.addLast(new LogAction("Executing run #" + (runNum + 1) + " ...", Level.INFO));
            ActionList runActions = new SequentialActionList("experiment run actions");
            super.assembleExperiment(runActions);
            execs.addLast(runActions);
        }
    }
}
