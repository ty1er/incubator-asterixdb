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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;

public class FieldExtractor<T> implements IFieldExtractor<T> {

    private ISerializerDeserializer serde;
    private int fieldIdx;
    private final String fieldName;

    public FieldExtractor(ISerializerDeserializer serde, int fieldIdx, String fieldName) {
        this.serde = serde;
        this.fieldIdx = fieldIdx;
        this.fieldName = fieldName;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ITypeTraits getFieldTypeTraits() {
        return SerdeUtils.serdeToTypeTrait(serde);
    }

    @Override
    public T extractFieldValue(ITupleReference tuple) throws HyracksDataException {
        ByteArrayInputStream inStream = new ByteArrayInputStream(tuple.getFieldData(fieldIdx),
                tuple.getFieldStart(fieldIdx), tuple.getFieldLength(fieldIdx));
        DataInput dataIn = new DataInputStream(inStream);
        return (T) serde.deserialize(dataIn);
    }
}
