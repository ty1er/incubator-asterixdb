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

package org.apache.hyracks.storage.am.statistics.common;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBuilder;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.wavelet.PrefixSumWaveletSynopsis;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletSynopsis;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletTransform;

public class StatisticsCollectorFactory implements IStatisticsFactory, Serializable {
    private static final Logger LOGGER = Logger.getLogger(StatisticsCollectorFactory.class.getName());

    private final int[] fields;
    private final int size;
    private final ITypeTraits[] fieldTypeTraits;
    private final SynopsisType type;
    private final IFieldExtractor fieldExtractor;

    public StatisticsCollectorFactory(SynopsisType type, int[] fields, int size, ITypeTraits[] fieldTypeTraits,
            IOrdinalPrimitiveValueProvider fieldValueProvider) {
        this.type = type;
        this.fields = fields;
        this.size = size;
        this.fieldTypeTraits = fieldTypeTraits;
        this.fieldExtractor = new FirstKeyFieldExtractor(fieldValueProvider, fields);
    }

    @Override
    public boolean canCollectStats() {
        if (fields.length > 1) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to collect statistics on composite keys");
            }
            return false;
        }
        if (!fieldTypeTraits[fields[0]].isFixedLength()) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to collect statistics for non-fixed length keys");
            }
            return false;
        }
        //check if the key can be mapped on long domain, i.e. key length  <= 8 bytes. 1 byte is reserved for typeTag
        if (fieldTypeTraits[fields[0]].getFixedLength() > (8 + 1)) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to collect statistics for keys with size greater than 8 bytes"
                        + fieldTypeTraits[fields[0]]);
            }
            return false;
        }
        return true;
    }

    @Override
    public ISynopsisBuilder createStatistics(ComponentStatistics componentStatistics, boolean isBulkload)
            throws HyracksDataException {
        AbstractSynopsisBuilder synopsisBuilder = createSynopsisBuilder(componentStatistics, false);
        if (isBulkload) {
            return synopsisBuilder;
        } else {
            AbstractSynopsisBuilder antimatterSynopsisBuilder = createSynopsisBuilder(componentStatistics, true);
            return new CombinedSynopsisBuilder(synopsisBuilder, antimatterSynopsisBuilder);
        }
    }

    private AbstractSynopsisBuilder createSynopsisBuilder(ComponentStatistics componentStatistics, boolean isAntimatter)
            throws HyracksDataException {
        ISynopsis synopsis = SynopsisFactory.createSynopsis(type, fieldTypeTraits[0],
                SynopsisFactory.createSynopsisElements(type, size),
                isAntimatter ? componentStatistics.getNumAntimatterTuples() : componentStatistics.getNumTuples(), size);
        switch (type) {
            case UniformHistogram:
            case ContinuousHistogram:
            case EquiWidthHistogram:
                return new HistogramBuilder((HistogramSynopsis<? extends HistogramBucket>) synopsis, isAntimatter,
                        fieldExtractor, componentStatistics);
            case PrefixSumWavelet:
                return new WaveletTransform((PrefixSumWaveletSynopsis) synopsis, isAntimatter, fieldExtractor,
                        componentStatistics);
            default:
                throw new HyracksDataException("Cannot instantiate new synopsis builder for type " + type);
        }
    }
}
