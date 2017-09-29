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

import java.util.Objects;

public class TransformTuple {
    public long position;
    public double value;
    public boolean isAntimatter;

    public TransformTuple(long x, double y, boolean isAntimatter) {
        this.position = x;
        this.value = y;
        this.isAntimatter = isAntimatter;
    }

    public TransformTuple(long x, double y) {
        this(x, y, false);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TransformTuple))
            return false;
        TransformTuple tuple = (TransformTuple) o;
        return tuple.position == position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }
}