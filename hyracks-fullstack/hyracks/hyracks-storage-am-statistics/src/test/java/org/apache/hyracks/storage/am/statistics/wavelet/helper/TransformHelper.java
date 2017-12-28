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
package org.apache.hyracks.storage.am.statistics.wavelet.helper;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsisBuilder;

public class TransformHelper {

    public static void runTransform(List<TransformTuple> inputData, AbstractSynopsisBuilder synopsisBuilder)
            throws Exception {
        Collections.sort(inputData, Comparator.comparingLong(o -> o.position));
        for (TransformTuple t : inputData) {
            for (int i = 0; i < t.cardinality; i++)
                synopsisBuilder.addValue(t.position);
        }
        synopsisBuilder.finishSynopsisBuild();
    }
}
