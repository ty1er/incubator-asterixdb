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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.AbstractListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.IACollection;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractListSerializerDeserializer<T extends IACollection> implements ISerializerDeserializer<T> {

    private static final long serialVersionUID = 1L;

    protected final IAType itemType;
    protected final AbstractCollectionType listType;
    @SuppressWarnings("rawtypes")
    protected final ISerializerDeserializer deserializer;
    @SuppressWarnings("rawtypes")
    protected final ISerializerDeserializer serializer;
    protected AbstractListBuilder listBuilder;

    public AbstractListSerializerDeserializer(AbstractCollectionType listType) {
        this.listType = listType;
        this.itemType = listType.getItemType();
        serializer = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
        deserializer = itemType.getTypeTag() == ATypeTag.ANY
                ? AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType)
                : AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(itemType);
    }

    public static final int getListLength(byte[] serOrderedList, int offset) {
        return AInt32SerializerDeserializer.getInt(serOrderedList, offset + 1);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void serialize(IACollection instance, DataOutput out) throws HyracksDataException {
        // TODO: schemaless ordered list serializer
        listBuilder.reset(listType);
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        IACursor cursor = instance.getCursor();
        while (cursor.next()) {
            itemValue.reset();
            serializer.serialize(cursor.get(), itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        listBuilder.write(out, false);
    }

    public List<IAObject> readList(DataInput in) throws HyracksDataException {
        List<IAObject> items = new ArrayList<IAObject>();
        listBuilder.reset(listType);
        listBuilder.read(in, deserializer, items);
        return items;
    }

    public static int getNumberOfItems(byte[] serOrderedList) {
        return getNumberOfItems(serOrderedList, 0);
    }

    public static int getNumberOfItems(byte[] serOrderedList, int offset) {
        // 6 = tag (1) + itemTag (1) + list size (4)
        return AInt32SerializerDeserializer.getInt(serOrderedList, offset + 6);
    }

    public static int getItemOffset(byte[] serOrderedList, int offset, int itemIndex) throws AsterixException {
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serOrderedList[offset + 1]);
        switch (typeTag) {
            case STRING:
            case BINARY:
            case RECORD:
            case ORDEREDLIST:
            case UNORDEREDLIST:
            case UNION:
            case ANY:
                return offset + AInt32SerializerDeserializer.getInt(serOrderedList, offset + 10 + (4 * itemIndex));
            default:
                int length = NonTaggedFormatUtil.getFieldValueLength(serOrderedList, offset + 1, typeTag, true);
                // 10 = tag (1) + itemTag (1) + list size (4) + number of items (4)
                return offset + 10 + (length * itemIndex);
        }
    }

    public static int getItemOffset(byte[] serOrderedList, int itemIndex) throws AsterixException {
        return getItemOffset(serOrderedList, 0, itemIndex);
    }

}