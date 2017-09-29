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

    private AqlOrdinalPrimitiveValueProviderFactory() {
    }

    @Override
    public IOrdinalPrimitiveValueProvider createOrdinalPrimitiveValueProvider() {
        return new IOrdinalPrimitiveValueProvider() {
            final IOrdinalPrimitiveValueProvider byteProvider = BytePrimitiveValueProviderFactory.INSTANCE
                    .createOrdinalPrimitiveValueProvider();
            final IOrdinalPrimitiveValueProvider shortProvider = ShortPrimitiveValueProviderFactory.INSTANCE
                    .createOrdinalPrimitiveValueProvider();
            final IOrdinalPrimitiveValueProvider intProvider = IntegerPrimitiveValueProviderFactory.INSTANCE
                    .createOrdinalPrimitiveValueProvider();
            final IOrdinalPrimitiveValueProvider longProvider = LongPrimitiveValueProviderFactory.INSTANCE
                    .createOrdinalPrimitiveValueProvider();
            final IOrdinalPrimitiveValueProvider dateTimeProvider = DateTimePrimitiveValueProviderFactory.INSTANCE
                    .createOrdinalPrimitiveValueProvider();
            final IOrdinalPrimitiveValueProvider dateProvider = DatePrimitiveValueProviderFactory.INSTANCE
                    .createOrdinalPrimitiveValueProvider();
            final IOrdinalPrimitiveValueProvider timeProvider =
                    TimePrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
            final IOrdinalPrimitiveValueProvider dayTimeDurationProvider =
                    DayTimeDurationPrimitiveValueProviderFactory.INSTANCE.createOrdinalPrimitiveValueProvider();
            final IOrdinalPrimitiveValueProvider yearMonthDurationProvider =
                    YearMonthDurationPrimitiveValueProviderFactory.INSTANCE
                    .createOrdinalPrimitiveValueProvider();

            @Override
            public long getOrdinalValue(byte[] bytes, int offset) {

                ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
                switch (tag) {
                    case TINYINT: {
                        return byteProvider.getOrdinalValue(bytes, offset + 1);
                    }
                    case SMALLINT: {
                        return shortProvider.getOrdinalValue(bytes, offset + 1);
                    }
                    case INTEGER: {
                        return intProvider.getOrdinalValue(bytes, offset + 1);
                    }
                    case BIGINT: {
                        return longProvider.getOrdinalValue(bytes, offset + 1);
                    }
                    case DATETIME: {
                        return dateTimeProvider.getOrdinalValue(bytes, offset + 1);
                    }
                    case DATE: {
                        return dateProvider.getOrdinalValue(bytes, offset + 1);
                    }
                    case TIME: {
                        return timeProvider.getOrdinalValue(bytes, offset + 1);
                    }
                    case DAYTIMEDURATION: {
                        return dayTimeDurationProvider.getOrdinalValue(bytes, offset + 1);
                    }
                    case YEARMONTHDURATION: {
                        return yearMonthDurationProvider.getOrdinalValue(bytes, offset + 1);
                    }
                    default: {
                        throw new NotImplementedException("Value provider for type " + tag + " is not implemented");
                    }
                }
            }
        };
    }
}
