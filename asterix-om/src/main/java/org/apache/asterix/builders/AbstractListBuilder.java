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
package org.apache.asterix.builders;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.storage.am.common.ophelpers.IntArrayList;

public abstract class AbstractListBuilder implements IAsterixListBuilder, Serializable {

    private static final long serialVersionUID = 1L;

    protected static final byte NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    protected int metadataInfoSize;
    protected int headerSize;
    protected IAType itemType;
    protected boolean nullable = false;
    protected ATypeTag listTypeTag;
    protected boolean fixedSize = false;
    protected int numberOfItems;
    protected transient final GrowableArray outputStorage;
    protected transient final DataOutputStream outputStream;
    protected transient final IntArrayList offsets;
    protected transient byte[] offsetArray;
    protected transient int offsetPosition;

    public AbstractListBuilder(ATypeTag listTypeTag) {
        this.listTypeTag = listTypeTag;
        this.offsets = new IntArrayList(10, 10);
        this.offsetArray = null;
        this.offsetPosition = 0;
        this.outputStorage = new GrowableArray();
        this.outputStream = (DataOutputStream) outputStorage.getDataOutput();
    }

    @Override
    public void reset(AbstractCollectionType listType) {
        this.metadataInfoSize = 0;
        this.numberOfItems = 0;
        this.headerSize = 2;
        if (listType == null || listType.getItemType() == null) {
            this.itemType = BuiltinType.ANY;
            fixedSize = false;
        } else {
            this.itemType = listType.getItemType();
            if (NonTaggedFormatUtil.isOptional(listType.getItemType())) {
                this.itemType = ((AUnionType) listType.getItemType()).getNullableType();
                nullable = true;
            }
            fixedSize = NonTaggedFormatUtil.isFixedSizedCollection(listType.getItemType());
        }
        metadataInfoSize = 8;
        // 8 = 4 (# of items) + 4 (the size of the list)
        this.offsets.clear();
        this.offsetArray = null;
        this.offsetPosition = 0;
        this.outputStorage.reset();
    }

    public void read(DataInput in, @SuppressWarnings("rawtypes") ISerializerDeserializer deserializer,
            Collection<IAObject> items) throws HyracksDataException {
        try {
            fixedSize = false;
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(in.readByte());
            switch (typeTag) {
                case STRING:
                case BINARY:
                case RECORD:
                case ORDEREDLIST:
                case UNORDEREDLIST:
                case UNION:
                case ANY:
                    fixedSize = false;
                    break;
                default:
                    fixedSize = true;
                    break;
            }

            IAType currentItemType = itemType;
            @SuppressWarnings("rawtypes")
            ISerializerDeserializer currentDeserializer = deserializer;
            if (itemType.equals(BuiltinType.ANY) && typeTag != ATypeTag.ANY) {
                currentItemType = TypeTagUtil.getBuiltinTypeByTag(typeTag);
                currentDeserializer = AqlSerializerDeserializerProvider.INSTANCE
                        .getNonTaggedSerializerDeserializer(currentItemType);
            }

            in.readInt(); // list size
            numberOfItems = in.readInt();
            if (numberOfItems > 0) {
                if (!fixedSize) {
                    for (int i = 0; i < numberOfItems; i++) {
                        in.readInt();
                    }
                }
                for (int i = 0; i < numberOfItems; i++) {
                    IAObject v = (IAObject) currentDeserializer.deserialize(in);
                    items.add(v);
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void addItem(IValueReference item) throws HyracksDataException {
        try {
            if (!fixedSize
                    && (item.getByteArray()[0] != NULL_TYPE_TAG || itemType.equals(BuiltinType.ANY) || nullable)) {
                this.offsets.add(outputStorage.getLength());
            }
            if (itemType.equals(BuiltinType.ANY) || nullable
                    || (itemType.equals(BuiltinType.ANULL) && item.getByteArray()[0] == NULL_TYPE_TAG)) {
                this.numberOfItems++;
                this.outputStream.write(item.getByteArray(), item.getStartOffset(), item.getLength());
            } else if (item.getByteArray()[0] != NULL_TYPE_TAG) {
                this.numberOfItems++;
                this.outputStream.write(item.getByteArray(), item.getStartOffset() + 1, item.getLength() - 1);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void write(DataOutput out, boolean writeTypeTag) throws HyracksDataException {
        try {
            if (!fixedSize) {
                metadataInfoSize += offsets.size() * 4;
            }
            if (offsetArray == null || offsetArray.length < metadataInfoSize) {
                offsetArray = new byte[metadataInfoSize];
            }

            SerializerDeserializerUtil.writeIntToByteArray(offsetArray,
                    headerSize + metadataInfoSize + outputStorage.getLength(), offsetPosition);
            SerializerDeserializerUtil.writeIntToByteArray(offsetArray, this.numberOfItems, offsetPosition + 4);

            if (!fixedSize) {
                offsetPosition += 8;
                for (int i = 0; i < offsets.size(); i++) {
                    SerializerDeserializerUtil.writeIntToByteArray(offsetArray,
                            offsets.get(i) + metadataInfoSize + headerSize, offsetPosition);
                    offsetPosition += 4;
                }
            }
            if (writeTypeTag) {
                out.writeByte(listTypeTag.serialize());
            }
            if (nullable) {
                out.writeByte(ATypeTag.UNION.serialize());
            } else {
                out.writeByte(itemType.getTypeTag().serialize());
            }
            out.write(offsetArray, 0, metadataInfoSize);
            out.write(outputStorage.getByteArray(), 0, outputStorage.getLength());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
