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
package org.apache.asterix.runtime.statistics;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntegerSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.Domain;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;

public class StatisticsUtil {
    private StatisticsUtil() {
    }

    public static List<IFieldExtractor> computeStatisticsFieldExtractors(ITypeTraitProvider typeTraitProvider,
            IPrimitiveValueProviderFactory primitiveFactory, ARecordType recordType, List<List<String>> indexKeys,
            boolean isPrimaryIndex, String[] unorderedStatisticsFields) throws AlgebricksException {
        // add statistics on indexed fields (primary or secondary keys)
        if (indexKeys.size() > 1) {
            throw new AsterixException("Cannot collect statistics on composite fields");
        }
        List<IFieldExtractor> result = new ArrayList<>();
        // TODO: allow nested fields
        String keyField = String.join(".", indexKeys.get(0));
        IAType keyType = recordType.getFieldType(keyField);
        if (ATypeHierarchy.belongsToDomain(keyType.getTypeTag(), Domain.INTEGER)) {
            AIntegerSerializerDeserializer serDe =
                    (AIntegerSerializerDeserializer) SerializerDeserializerProvider.INSTANCE
                            .getNonTaggedSerializerDeserializer(recordType.getFieldType(keyField));
            result.add(new FieldExtractor(serDe, 0, keyField, typeTraitProvider.getTypeTrait(keyType)));
        }
        // add statistics on non-indexed fields
        if (isPrimaryIndex && unorderedStatisticsFields != null && unorderedStatisticsFields.length > 0) {
            for (int i = 0; i < unorderedStatisticsFields.length; i++) {
                recordType.getFieldType(unorderedStatisticsFields[i]);
                int statisticsFieldIdx = recordType.getFieldIndex(unorderedStatisticsFields[i]);
                ITypeTraits statisticsTypeTraits =
                        typeTraitProvider.getTypeTrait(recordType.getFieldTypes()[statisticsFieldIdx]);
                result.add(getFieldExtractor(primitiveFactory, recordType, statisticsFieldIdx,
                        unorderedStatisticsFields[i], statisticsTypeTraits));
            }
        }
        return result;
    }

    public static IFieldExtractor getFieldExtractor(IPrimitiveValueProviderFactory primitiveFactory,
            ARecordType recordType, int statisticsFieldIdx, String statisticsFieldName, ITypeTraits typeTraits) {
        //incoming tuple has format [PK][Record]... and we need to extract the record, i.e. 2nd field
        final int hyracksFieldIdx = 1;
        return new IFieldExtractor() {
            private final IPrimitiveValueProvider primitiveProvider = primitiveFactory.createPrimitiveValueProvider();
            private final ARecordPointable recPointable = ARecordPointable.FACTORY.createPointable();

            @Override
            public String getFieldName() {
                return statisticsFieldName;
            }

            @Override
            public ITypeTraits getFieldTypeTraits() {
                return typeTraits;
            }

            @Override
            public Long extractFieldValue(ITupleReference tuple) throws HyracksDataException {
                if (tuple.getFieldCount() < hyracksFieldIdx) {
                    throw new HyracksDataException(
                            "Cannot extract field " + hyracksFieldIdx + " from incoming hyracks tuple");
                }
                recPointable.set(tuple.getFieldData(hyracksFieldIdx), tuple.getFieldStart(hyracksFieldIdx),
                        tuple.getFieldLength(hyracksFieldIdx));
                return primitiveProvider.getLongValue(recPointable.getByteArray(),
                        recPointable.getClosedFieldOffset(recordType, statisticsFieldIdx));

            }
        };
    }
}
