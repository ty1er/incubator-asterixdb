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
package org.apache.asterix.external.parser;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class BinaryParser implements IRecordDataParser<byte[]> {
    private final ARecordType recordType;
    private RecordBuilder recBuilder;
    private ArrayBackedValueStorage recordValue = new ArrayBackedValueStorage();
    private IPointable recordPointable;
    private IPointable[] fieldPointables;

    public BinaryParser(ARecordType recordType) throws HyracksDataException {
        this.recordType = recordType;
        this.recBuilder = new RecordBuilder();

        recBuilder.reset(recordType);
        recBuilder.init();
        int recordLength = 0;
        fieldPointables = new IPointable[recordType.getFieldNames().length];
        //2st loop to calculate total records length
        for (int i = 0; i < recordType.getFieldNames().length; i++) {
            IAType fieldType = recordType.getFieldTypes()[i];
            ITypeTraits typeTraits = TypeTraitProvider.INSTANCE.getTypeTrait(fieldType);
            if (!typeTraits.isFixedLength())
                throw new HyracksDataException("Cannot parse non-fixed length types");
            fieldPointables[i] = getFieldPointable(fieldType.getTypeTag());
            //subtract 1 to accommodate for extra typeTag byte
            int fieldLength = typeTraits.getFixedLength() - 1;
            recordLength += fieldLength;
        }
        recordValue.setSize(recordLength);
        recordLength = 0;
        //2nd loop to initialize field pointables
        for (int i = 0; i < recordType.getFieldNames().length; i++) {
            IAType fieldType = recordType.getFieldTypes()[i];
            ITypeTraits typeTraits = TypeTraitProvider.INSTANCE.getTypeTrait(fieldType);
            int fieldLength = typeTraits.getFixedLength() - 1;
            fieldPointables[i].set(recordValue.getByteArray(), recordLength, fieldLength);
            recordLength += fieldLength;
        }
        recordPointable = new AbstractPointable() {
            @Override
            public void set(byte[] bytes, int start, int length) {
                super.set(bytes, start, length);
            }
        };
    }

    private IPointable getFieldPointable(ATypeTag fieldType) throws HyracksDataException {
        switch (fieldType) {
            case TINYINT:
                return BytePointable.FACTORY.createPointable();
            case SMALLINT:
                return ShortPointable.FACTORY.createPointable();
            case INTEGER:
                return IntegerPointable.FACTORY.createPointable();
            case BIGINT:
                return LongPointable.FACTORY.createPointable();
            default:
                throw new HyracksDataException(
                        "Pointable for file type " + fieldType.toString() + " cannot be created");
        }
    }

    @Override
    public void parse(IRawRecord<? extends byte[]> record, DataOutput out) throws HyracksDataException {
        try {
            recBuilder.reset(recordType);
            recBuilder.init();
            recordPointable.set(record.getBytes(), 0, record.size());
            recordValue.assign(recordPointable);
            for (int i = 0; i < recordType.getFieldNames().length; i++) {
                recBuilder.addNonTaggedField(i, fieldPointables[i]);
            }
            recBuilder.write(out, true);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
