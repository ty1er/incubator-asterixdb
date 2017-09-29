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
package org.apache.asterix.dataflow.data.nontagged.valueproviders;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.impls.BytePrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.impls.IntegerPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.impls.LongPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.impls.ShortPrimitiveValueProviderFactory;

public class AqlOrdinalPrimitiveValueProviderFactory implements IOrdinalPrimitiveValueProviderFactory {

    private static final long serialVersionUID = 1L;

    public static final AqlOrdinalPrimitiveValueProviderFactory INSTANCE =
            new AqlOrdinalPrimitiveValueProviderFactory();

    private static final IOrdinalPrimitiveValueProvider byteProvider =
            BytePrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
    private static final IOrdinalPrimitiveValueProvider shortProvider =
            ShortPrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
    private static final IOrdinalPrimitiveValueProvider intProvider =
            IntegerPrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
    private static final IOrdinalPrimitiveValueProvider longProvider =
            LongPrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
    private static final IOrdinalPrimitiveValueProvider dateTimeProvider =
            DateTimePrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
    private static final IOrdinalPrimitiveValueProvider dateProvider =
            DatePrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
    private static final IOrdinalPrimitiveValueProvider timeProvider =
            TimePrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
    private static final IOrdinalPrimitiveValueProvider dayTimeDurationProvider =
            DayTimeDurationPrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
    private static final IOrdinalPrimitiveValueProvider yearMonthDurationProvider =
            YearMonthDurationPrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();

    private AqlOrdinalPrimitiveValueProviderFactory() {
    }

    @Override
    public IOrdinalPrimitiveValueProvider createOrdinalPrimitiveValueProvider() {
        return (IOrdinalPrimitiveValueProvider) (bytes, offset) -> {
            ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
            return getTaggedOrdinalValue(tag, bytes, offset + 1);
        };
    }

    public static long getTaggedOrdinalValue(ATypeTag tag, byte[] bytes, int offset) {
        switch (tag) {
            case TINYINT: {
                return byteProvider.getOrdinalValue(bytes, offset);
            }
            case SMALLINT: {
                return shortProvider.getOrdinalValue(bytes, offset);
            }
            case INTEGER: {
                return intProvider.getOrdinalValue(bytes, offset);
            }
            case BIGINT: {
                return longProvider.getOrdinalValue(bytes, offset);
            }
            case DATETIME: {
                return dateTimeProvider.getOrdinalValue(bytes, offset);
            }
            case DATE: {
                return dateProvider.getOrdinalValue(bytes, offset);
            }
            case TIME: {
                return timeProvider.getOrdinalValue(bytes, offset);
            }
            case DAYTIMEDURATION: {
                return dayTimeDurationProvider.getOrdinalValue(bytes, offset);
            }
            case YEARMONTHDURATION: {
                return yearMonthDurationProvider.getOrdinalValue(bytes, offset);
            }
            default: {
                throw new NotImplementedException("Value provider for type " + tag + " is not implemented");
            }
        }
    }
}
