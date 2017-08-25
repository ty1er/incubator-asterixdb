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

package org.apache.asterix.experiment.client;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

public class SocketTweetGeneratorStatisticsConfig extends TweetGeneratorConfig {

    @Option(name = "-d", aliases = { "--datagen-duration" }, usage = "Duration in seconds to run data generation")
    private int duration = -1;

    public int getDatagenDuration() {
        return duration;
    }

    @Option(name = "-qd", aliases = { "--querygen-duration" }, usage = "Duration in seconds to run query generation")
    private int queryDuration = -1;

    public int getQueryGenDuration() {
        return queryDuration;
    }

    @Option(name = "-ni", aliases = "--num-intervals", usage = "Number of intervals to use when generating data based on data size (default = 1)")
    private int nIntervals = 1;

    public int getNIntervals() {
        return nIntervals;
    }

    @Option(name = "-rcbi", aliases = "--record-count-per-batch-during-ingestion-only", usage = "Record count per batch during ingestion only")
    private int recordCountPerBatchDuringIngestionOnly = 1000;

    public int getRecordCountPerBatchDuringIngestionOnly() {
        return recordCountPerBatchDuringIngestionOnly;
    }

    @Option(name = "-rcbq", aliases = "--record-count-per-batch-during-query", usage = "Record count per batch during query")
    private int recordCountPerBatchDuringQuery = 1000;

    public int getRecordCountPerBatchDuringQuery() {
        return recordCountPerBatchDuringQuery;
    }

    @Option(name = "-dsti", aliases = "--data-gen-sleep-time-during-ingestion-only", usage = "DataGen sleep time in milliseconds after every recordCountPerBatchDuringIngestionOnly records were sent")
    private long dataGenSleepTimeDuringIngestionOnly = 1;

    public long getDataGenSleepTimeDuringIngestionOnly() {
        return dataGenSleepTimeDuringIngestionOnly;
    }

    @Option(name = "-dstq", aliases = "--data-gen-sleep-time-during-query", usage = "DataGen sleep time in milliseconds after every recordCountPerBatchDuringQuery records were sent")
    private long dataGenSleepTimeDuringQuery = 1;

    public long getDataGenSleepTimeDuringQuery() {
        return dataGenSleepTimeDuringQuery;
    }

    @Argument(usage = "A list of <ip>:<port> pairs (addresses) to send data to", metaVar = "addresses...", handler = AddressOptionHandler.class, index = 0)
    private List<Pair<String, Integer>> addresses;

    public List<Pair<String, Integer>> getAddresses() {
        return addresses;
    }

    public static class AddressOptionHandler extends OptionHandler<Pair<String, Integer>> {

        public AddressOptionHandler(CmdLineParser parser, OptionDef option,
                Setter<? super Pair<String, Integer>> setter) {
            super(parser, option, setter);
        }

        @Override
        public int parseArguments(Parameters params) throws CmdLineException {
            int counter = 0;
            while (true) {
                String param;
                try {
                    param = params.getParameter(counter);
                } catch (CmdLineException ex) {
                    break;
                }

                String[] hostPort = param.split(":");
                if (hostPort.length != 2) {
                    System.err.println("Invalid address: " + param + ". Expected <host>:<port>");
                    System.exit(1);
                }
                Integer port = null;
                try {
                    port = Integer.parseInt(hostPort[1]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port " + hostPort[1] + " for address " + param + ".");
                    System.exit(1);
                }
                setter.addValue(Pair.of(hostPort[0], port));
                counter++;
            }
            return counter;
        }

        @Override
        public String getDefaultMetaVariable() {
            return "addresses";
        }

    }
}
