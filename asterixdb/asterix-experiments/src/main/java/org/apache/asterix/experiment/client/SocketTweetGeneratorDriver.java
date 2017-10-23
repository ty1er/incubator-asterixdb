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

import static org.apache.asterix.experiment.client.GeneratorFactory.KEY_MIN_SPREAD;

import java.util.HashMap;
import java.util.Map;

public class SocketTweetGeneratorDriver {
    public static void main(String[] args) throws Exception {
        SocketTweetGeneratorStatisticsConfig config = new SocketTweetGeneratorStatisticsConfig();
        config = DriverUtils.getCmdParams(args, config);

        if ((config.getDatagenCount() == -1 && config.getDatagenDuration() == -1) || (config.getDatagenCount() > 0
                && config.getDatagenDuration() > 0)) {
            System.err.println("Must use exactly one of -d(--datagen-duration) or -dc(--datagen-count)");
            System.exit(1);
        }

        Map<String, String> properties = new HashMap<>();
        properties.put(GeneratorFactory.KEY_ZIPF_SKEW, config.getSkew().toString());
        properties.put(KEY_MIN_SPREAD, Integer.toString(RangeGenerator.MIN_SPREAD));

        SocketTweetGenerator client = new SocketTweetGenerator(config, properties);
        client.start();
    }
}
