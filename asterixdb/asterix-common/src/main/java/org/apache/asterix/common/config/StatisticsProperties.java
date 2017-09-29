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
package org.apache.asterix.common.config;

import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;

import org.apache.asterix.common.statistics.SynopsisMergeStrategy;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;

public class StatisticsProperties extends AbstractProperties {

    public enum Option implements IOption {

        STATISTICS_SYNOPSIS_TYPE(STRING, "None", "The type of synopsis used for collecting dataset statistics"),
        STATISTICS_SYNOPSIS_SIZE(
                INTEGER,
                100,
                "The size of synopsis (number of histogram buckets or coefficients in wavelet transform)."),
        STATISTICS_MERGE_STRATEGY(STRING, "TimeBased", "Statistics merge strategy (if applicable)"),
        STATISTICS_MERGE_STRATEGY_TIMEOUT(
                INTEGER,
                60000,
                "The timeout in milliseconds between the statistics merge operation invocations");

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        Option(IOptionType type, Object defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            return Section.NC;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public IOptionType type() {
            return type;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }
    }

    public StatisticsProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public SynopsisType getStatisticsSynopsisType() {
        return SynopsisType.valueOf(accessor.getString(Option.STATISTICS_SYNOPSIS_TYPE));
    }

    public int getStatisticsSize() {
        return accessor.getInt(Option.STATISTICS_SYNOPSIS_SIZE);
    }

    public SynopsisMergeStrategy getStatisticsMergeStrategy() {
        return SynopsisMergeStrategy.valueOf(accessor.getString(Option.STATISTICS_MERGE_STRATEGY));
    }

    public int getStatisticsMergeTimeout() {
        return accessor.getInt(Option.STATISTICS_MERGE_STRATEGY_TIMEOUT);
    }
}
