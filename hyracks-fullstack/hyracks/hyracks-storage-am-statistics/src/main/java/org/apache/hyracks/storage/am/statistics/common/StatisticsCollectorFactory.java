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

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatistics;
import org.apache.hyracks.storage.am.statistics.historgram.EquiHeightHistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBuilder;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.sketch.GroupCountSketchBuilder;
import org.apache.hyracks.storage.am.statistics.sketch.quantile.QuantileSketchBuilder;
import org.apache.hyracks.storage.am.statistics.wavelet.PrefixSumWaveletSynopsis;
import org.apache.hyracks.storage.am.statistics.wavelet.PrefixSumWaveletTransform;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletSynopsis;
import org.apache.hyracks.storage.am.statistics.wavelet.WaveletTransform;

public class StatisticsCollectorFactory extends AbstractStatisticsFactory {
    private static final Logger LOGGER = Logger.getLogger(StatisticsCollectorFactory.class.getName());

    private final String dataverseName;
    private final String datasetName;
    private final String indexName;
    private final int size;
    private final double energyAccuracy;
    private final SynopsisType type;
    private final int fanout;
    private double failureProbability;
    private double accuracy;

    public StatisticsCollectorFactory(SynopsisType type, String dataverseName, String datasetName, String indexName,
            List<String> fields, List<ITypeTraits> fieldTypeTraits, List<IFieldExtractor> fieldValueExtractors,
            int size, int fanout, double failureProbability, double accuracy, double energyAccuracy) {
        super(fields, fieldTypeTraits, fieldValueExtractors);
        this.type = type;
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.indexName = indexName;
        this.size = size;
        this.fanout = fanout;
        this.failureProbability = failureProbability;
        this.accuracy = accuracy;
        this.energyAccuracy = energyAccuracy;

    }

    @Override
    public boolean canCollectStats(boolean unorderedTuples) {
        if (unorderedTuples && type.needsSortedOrder()) {
            return false;
        }
        for (ITypeTraits typeTraits : fieldTypeTraits) {
            //check if the key can be mapped on long domain, i.e. key length  <= 8 bytes. 1 byte is reserved for typeTag
            if (typeTraits.getFixedLength() > (8 + 1)) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unable to collect statistics for keys with size greater than 8 bytes" + typeTraits);
                }
                return false;
            }
        }
        return true;
    }

    protected AbstractSynopsisBuilder createSynopsisBuilder(ComponentStatistics componentStatistics,
            boolean isAntimatter, String fieldName, ITypeTraits fieldTraits, IFieldExtractor fieldExtractor)
            throws HyracksDataException {
        long numElements =
                isAntimatter ? componentStatistics.getNumAntimatterTuples() : componentStatistics.getNumTuples();
        ISynopsis synopsis = SynopsisFactory.createSynopsis(type, fieldTraits,
                SynopsisElementFactory.createSynopsisElementsCollection(type, size), numElements, size);
        switch (type) {
            case UniformHistogram:
            case ContinuousHistogram:
            case EquiWidthHistogram:
                return new HistogramBuilder((HistogramSynopsis<? extends HistogramBucket>) synopsis, dataverseName,
                        datasetName, indexName, fieldName, isAntimatter, fieldExtractor, componentStatistics);
            case PrefixSumWavelet:
                return new PrefixSumWaveletTransform((PrefixSumWaveletSynopsis) synopsis, dataverseName, datasetName,
                        indexName, fieldName, isAntimatter, fieldExtractor, componentStatistics);
            case Wavelet:
                return new WaveletTransform((WaveletSynopsis) synopsis, dataverseName, datasetName, indexName,
                        fieldName, isAntimatter, fieldExtractor, componentStatistics);
            case GroupCountSketch:
                return new GroupCountSketchBuilder((WaveletSynopsis) synopsis, dataverseName, datasetName, indexName,
                        fieldName, isAntimatter, fieldExtractor, componentStatistics, fanout, failureProbability,
                        accuracy, energyAccuracy, numElements, (int) System.currentTimeMillis());
            case QuantileSketch:
                return new QuantileSketchBuilder((EquiHeightHistogramSynopsis) synopsis, dataverseName, datasetName,
                        indexName, fieldName, isAntimatter, fieldExtractor, componentStatistics, accuracy);
            default:
                throw new HyracksDataException("Cannot instantiate new synopsis builder for type " + type);
        }
    }
}
