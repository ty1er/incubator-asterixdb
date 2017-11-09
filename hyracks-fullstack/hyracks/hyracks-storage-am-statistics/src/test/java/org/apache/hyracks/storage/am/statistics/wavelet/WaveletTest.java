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
package org.apache.hyracks.storage.am.statistics.wavelet;

public abstract class WaveletTest {
    protected final long domainStart;
    protected final long domainEnd;
    protected final int maxLevel;
    protected final int threshold;
    protected final boolean normalize;

    protected static double epsilon = 0.001;

    public WaveletTest(int threshold, int maxLevel, boolean normalize, long domainStart, long domainEnd) {
        this.threshold = threshold;
        this.maxLevel = maxLevel;
        this.normalize = normalize;
        this.domainStart = domainStart;
        this.domainEnd = domainEnd;
    }
}
