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

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;

public class WaveletCoefficient implements ISynopsisElement {

    private static final long serialVersionUID = 1L;

    private double value = 0.0;
    private int level = -1;
    private long index = -1L;

    public static Comparator<WaveletCoefficient> KEY_COMPARATOR = new KeyComparator();

    private static class KeyComparator implements Comparator<WaveletCoefficient>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(WaveletCoefficient o1, WaveletCoefficient o2) {
            return Long.compareUnsigned(o1.getKey(), o2.getKey());
        }
    }

    public static Comparator<WaveletCoefficient> VALUE_COMPARATOR = new ValueComparator();

    private static class ValueComparator implements Comparator<WaveletCoefficient>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        // default comparator based on absolute coefficient value
        public int compare(WaveletCoefficient o1, WaveletCoefficient o2) {
            return Double.compare(Math.abs(o1.getValue()), Math.abs(o2.getValue()));
        }
    };

    public WaveletCoefficient(double value, int level, long index) {
        this.value = value;
        this.level = level;
        this.index = index;
    }

    @Override
    public long getKey() {
        return index;
    }

    @Override
    public double getValue() {
        return value;
    }

    public int getLevel() {
        return level;
    }

    public Double setValue(Double value) {
        Double oldValue = this.value;
        this.value = value;
        return oldValue;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WaveletCoefficient)) {
            return false;
        }
        WaveletCoefficient coeff = (WaveletCoefficient) o;
        return (coeff.value - value) < DoublePointable.getEpsilon() && (coeff.level == level) && (coeff.index == index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, level, index);
    }

    public boolean isDummy() {
        return level < 0;
    }

    public static double getNormalizationCoefficient(int maxLevel, int level) {
        return (1 << ((maxLevel - level) / 2)) * ((((maxLevel - level) % 2) == 0) ? 1 : Math.sqrt(2));
    }

    // Returns index of the parent coefficient
    public long getParentCoeffIndex(long domainMin, int maxLevel) {
        // Special case for values on level 0
        if (level == 0) {
            // Convert position to proper coefficient index
            //return (index >> 1) - (domainMin >> 1) + (1l << (maxLevel - 1));
            return (index - domainMin) >> 1 | (1l << (maxLevel - 1));
        } else {
            return index >>> 1;
        }
    }

    public long getLeftChildCoeffIndex(long domainMin, int maxLevel) {
        if (level == 1) {
            return -1;// (index << 1) - (domainMin >> 1) + (1l << (maxLevel - 1));
        } else {
            return index << 1;
        }
    }

    public long getRightChildCoeffIndex(long domainMin, int maxLevel) {
        return getLeftChildCoeffIndex(domainMin, maxLevel) + 1;
    }

    // Returns true if the coefficient's dyadic range covers tuple with the given position
    public boolean covers(long tuplePosition, int maxLevel, long domainMin) {
        if (level < 0) {
            return true;
        } else if (level == 0) {
            return index == tuplePosition;
        } else {
            return index == (((tuplePosition - domainMin) >>> 1) + (1l << (maxLevel - 1))) >>> (level - 1);
        }
    }

    // Method calculates coefficient level based on it's index and maximum level of transform. Since coefficient level
    // is a transient information the method is used to initialize coefficient with appropriate level value.
    public static int getLevel(long coeffIdx, int maxLevel) {
        if (coeffIdx == -1) {
            return -1;
        }
        if (coeffIdx == 0) {
            return maxLevel;
        }
        int level = 0;
        coeffIdx = coeffIdx >>> 1;
        while (coeffIdx > 0) {
            coeffIdx = coeffIdx >>> 1;
            level++;
        }
        return maxLevel - level;
    }
}