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

import org.kohsuke.args4j.Option;

public class WorldCupQueryGeneratorConfig extends QueryGeneratorConfig {

    @Option(name = "-rl", aliases = "--range-length", usage = "The range for fix-sized range queries")
    private int rangeLength;

    public int getRangeLength() {
        return rangeLength;
    }

    @Option(name = "-ub", aliases = "--upper-bounds", handler = LongArrayOptionHandler.class, usage = "Upper bound of the query range")
    private long[] upperBounds = { Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
            Byte.MAX_VALUE, Byte.MAX_VALUE };

    public long[] getUpperBounds() {
        return upperBounds;
    }

    @Option(name = "-lb", aliases = "--lower-bounds", handler = LongArrayOptionHandler.class, usage = "Lower bound of the query range")
    private long[] lowerBounds = { Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE,
            Byte.MIN_VALUE, Byte.MIN_VALUE };

    public long[] getLowerBounds() {
        return lowerBounds;
    }
}
