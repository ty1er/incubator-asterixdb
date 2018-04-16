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

package org.apache.asterix.experiment.builder.perf;

import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.RunQueryFileAction;
import org.apache.asterix.experiment.builder.AbstractPerfLoadBuilder;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunnerConfig;

import java.io.IOException;

public class PerfTestAggBuilder extends AbstractPerfLoadBuilder {

    public PerfTestAggBuilder(LSMExperimentSetRunnerConfig config) {
        super("PerfTestAggBuilder", config, "asterix-agg.xml", "asterix-4.dgen", "bench_count.aql", "bench_3_load.aql",
                "agg_bench");
    }

    @Override
    protected void doBuildDDL(SequentialActionList seq) throws IOException {
        seq.addLast(new RunQueryFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("bench_3.aql")));
    }

}
