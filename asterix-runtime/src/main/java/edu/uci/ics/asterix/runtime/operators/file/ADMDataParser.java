/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.List;
import java.util.Queue;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.IAsterixListBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.builders.UnorderedListBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APoint3DSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.hierachy.ATypeHierarchy;
import edu.uci.ics.asterix.om.types.hierachy.ITypePromoteComputer;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.operators.file.adm.AdmLexer;
import edu.uci.ics.asterix.runtime.operators.file.adm.AdmLexerException;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * Parser for ADM formatted data.
 */
public class ADMDataParser extends AbstractDataParser implements IDataParser {

    protected AdmLexer admLexer;
    protected ARecordType recordType;
    protected boolean datasetRec;

    private int nullableFieldId = 0;
    private ArrayBackedValueStorage castBuffer = new ArrayBackedValueStorage();
    
    private Queue<ArrayBackedValueStorage> baaosPool = new ArrayDeque<ArrayBackedValueStorage>();
    private Queue<IARecordBuilder> recordBuilderPool = new ArrayDeque<IARecordBuilder>();
    private Queue<IAsterixListBuilder> orderedListBuilderPool = new ArrayDeque<IAsterixListBuilder>();
    private Queue<IAsterixListBuilder> unorderedListBuilderPool = new ArrayDeque<IAsterixListBuilder>();

    private String mismatchErrorMessage = "Mismatch Type, expecting a value of type ";
    private String mismatchErrorMessage2 = " got a value of type ";

    @Override
    public boolean parse(DataOutput out) throws HyracksDataException {
        try {
            return parseAdmInstance((IAType) recordType, datasetRec, out);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void initialize(InputStream in, ARecordType recordType, boolean datasetRec) throws AsterixException {
        this.recordType = recordType;
        this.datasetRec = datasetRec;
        try {
            admLexer = new AdmLexer(new java.io.InputStreamReader(in));
        } catch (IOException e) {
            throw new AsterixException(e);
        }
    }

    protected boolean parseAdmInstance(IAType objectType, boolean datasetRec, DataOutput out) throws AsterixException,
            IOException {
        int token;
        try {
            token = admLexer.next();
        } catch (AdmLexerException e) {
            throw new AsterixException(e);
        }
        if (token == AdmLexer.TOKEN_EOF) {
            return false;
        } else {
            admFromLexerStream(token, objectType, out, datasetRec);
            return true;
        }
    }

    private void admFromLexerStream(int token, IAType objectType, DataOutput out, Boolean datasetRec)
            throws AsterixException, IOException {

        switch (token) {
            case AdmLexer.TOKEN_NULL_LITERAL: {
                if (checkType(ATypeTag.NULL, objectType, out)) {
                    nullSerde.serialize(ANull.NULL, out);
                } else
                    throw new AsterixException(" This field can not be null ");
                break;
            }
            case AdmLexer.TOKEN_TRUE_LITERAL: {
                if (checkType(ATypeTag.BOOLEAN, objectType, out)) {
                    booleanSerde.serialize(ABoolean.TRUE, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexer.TOKEN_BOOLEAN_CONS: {
                parseConstructor(ATypeTag.BOOLEAN, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_FALSE_LITERAL: {
                if (checkType(ATypeTag.BOOLEAN, objectType, out)) {
                    booleanSerde.serialize(ABoolean.FALSE, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexer.TOKEN_DOUBLE_LITERAL: {
                parseToNumericTarget(ATypeTag.DOUBLE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_DOUBLE_CONS: {
                parseConstructor(ATypeTag.DOUBLE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_FLOAT_LITERAL: {
                parseToNumericTarget(ATypeTag.FLOAT, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_FLOAT_CONS: {
                parseConstructor(ATypeTag.FLOAT, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT8_LITERAL: {
                parseAndCastNumeric(ATypeTag.INT8, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT8_CONS: {
                parseConstructor(ATypeTag.INT8, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT16_LITERAL: {
                parseAndCastNumeric(ATypeTag.INT16, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT16_CONS: {
                parseConstructor(ATypeTag.INT16, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT_LITERAL:{
                parseToNumericTarget(ATypeTag.INT32, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT32_LITERAL: {
                parseAndCastNumeric(ATypeTag.INT32, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT32_CONS: {
                parseConstructor(ATypeTag.INT32, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT64_LITERAL: {
                parseAndCastNumeric(ATypeTag.INT64, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INT64_CONS: {
                parseConstructor(ATypeTag.INT64, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_STRING_LITERAL: {
                if (checkType(ATypeTag.STRING, objectType, out)) {
                    aString.setValue(admLexer.getLastTokenImage().substring(1,
                            admLexer.getLastTokenImage().length() - 1));
                    stringSerde.serialize(aString, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexer.TOKEN_STRING_CONS: {
                parseConstructor(ATypeTag.STRING, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_DATE_CONS: {
                parseConstructor(ATypeTag.DATE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_TIME_CONS: {
                parseConstructor(ATypeTag.TIME, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_DATETIME_CONS: {
                parseConstructor(ATypeTag.DATETIME, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_INTERVAL_DATE_CONS: {
                try {
                    if (checkType(ATypeTag.INTERVAL, objectType, out)) {
                        if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_OPEN) {
                            if (admLexer.next() == AdmLexer.TOKEN_STRING_LITERAL) {
                                AIntervalSerializerDeserializer.parseDate(admLexer.getLastTokenImage(), out);

                                if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_CLOSE) {
                                    break;
                                }
                            }
                        }
                    }
                } catch (AdmLexerException ex) {
                    throw new AsterixException(ex);
                }
                throw new AsterixException("Wrong interval data parsing for date interval.");
            }
            case AdmLexer.TOKEN_INTERVAL_TIME_CONS: {
                try {
                    if (checkType(ATypeTag.INTERVAL, objectType, out)) {
                        if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_OPEN) {
                            if (admLexer.next() == AdmLexer.TOKEN_STRING_LITERAL) {
                                AIntervalSerializerDeserializer.parseTime(admLexer.getLastTokenImage(), out);

                                if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_CLOSE) {
                                    break;
                                }
                            }
                        }
                    }
                } catch (AdmLexerException ex) {
                    throw new AsterixException(ex);
                }
                throw new AsterixException("Wrong interval data parsing for time interval.");
            }
            case AdmLexer.TOKEN_INTERVAL_DATETIME_CONS: {
                try {
                    if (checkType(ATypeTag.INTERVAL, objectType, out)) {
                        if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_OPEN) {
                            if (admLexer.next() == AdmLexer.TOKEN_STRING_LITERAL) {
                                AIntervalSerializerDeserializer.parseDatetime(admLexer.getLastTokenImage(), out);

                                if (admLexer.next() == AdmLexer.TOKEN_CONSTRUCTOR_CLOSE) {
                                    break;
                                }
                            }
                        }
                    }
                } catch (AdmLexerException ex) {
                    throw new AsterixException(ex);
                }
                throw new AsterixException("Wrong interval data parsing for datetime interval.");
            }
            case AdmLexer.TOKEN_DURATION_CONS: {
                parseConstructor(ATypeTag.DURATION, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_YEAR_MONTH_DURATION_CONS: {
                parseConstructor(ATypeTag.YEARMONTHDURATION, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_DAY_TIME_DURATION_CONS: {
                parseConstructor(ATypeTag.DAYTIMEDURATION, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_POINT_CONS: {
                parseConstructor(ATypeTag.POINT, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_POINT3D_CONS: {
                parseConstructor(ATypeTag.POINT3D, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_CIRCLE_CONS: {
                parseConstructor(ATypeTag.CIRCLE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_RECTANGLE_CONS: {
                parseConstructor(ATypeTag.RECTANGLE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_LINE_CONS: {
                parseConstructor(ATypeTag.LINE, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_POLYGON_CONS: {
                parseConstructor(ATypeTag.POLYGON, objectType, out);
                break;
            }
            case AdmLexer.TOKEN_START_UNORDERED_LIST: {
                if (checkType(ATypeTag.UNORDEREDLIST, objectType, out)) {
                    objectType = getComplexType(objectType, ATypeTag.UNORDEREDLIST);
                    parseUnorderedList((AUnorderedListType) objectType, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeTag());
                break;
            }

            case AdmLexer.TOKEN_START_ORDERED_LIST: {
                if (checkType(ATypeTag.ORDEREDLIST, objectType, out)) {
                    objectType = getComplexType(objectType, ATypeTag.ORDEREDLIST);
                    parseOrderedList((AOrderedListType) objectType, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeTag());
                break;
            }
            case AdmLexer.TOKEN_START_RECORD: {
                if (checkType(ATypeTag.RECORD, objectType, out)) {
                    objectType = getComplexType(objectType, ATypeTag.RECORD);
                    parseRecord((ARecordType) objectType, out, datasetRec);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeTag());
                break;
            }
            case AdmLexer.TOKEN_EOF: {
                break;
            }
            default: {
                throw new AsterixException("Unexpected ADM token kind: " + AdmLexer.tokenKindToString(token) + ".");
            }
        }
    }

    private void parseDatetime(String datetime, DataOutput out) throws AsterixException, IOException {
        try {
            ADateTimeSerializerDeserializer.parse(datetime, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseDuration(String duration, DataOutput out) throws AsterixException {
        try {
            ADurationSerializerDeserializer.parse(duration, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }

    }

    private void parseYearMonthDuration(String duration, DataOutput out) throws AsterixException {
        try {
            AYearMonthDurationSerializerDeserializer.INSTANCE.parse(duration, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseDayTimeDuration(String duration, DataOutput out) throws AsterixException {
        try {
            ADayTimeDurationSerializerDeserializer.INSTANCE.parse(duration, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private IAType getComplexType(IAType aObjectType, ATypeTag tag) {

        if (aObjectType == null) {
            return null;
        }

        if (aObjectType.getTypeTag() == tag)
            return aObjectType;

        if (aObjectType.getTypeTag() == ATypeTag.UNION) {
            unionList = ((AUnionType) aObjectType).getUnionList();
            for (int i = 0; i < unionList.size(); i++)
                if (unionList.get(i).getTypeTag() == tag) {
                    return unionList.get(i);
                }
        }
        return null; // wont get here
    }

    List<IAType> unionList;

    private ATypeTag getTargetTypeTag(ATypeTag expectedTypeTag, IAType aObjectType) throws IOException {
        if (aObjectType == null) {
            return expectedTypeTag;
        }
        if (aObjectType.getTypeTag() != ATypeTag.UNION) {
            final ATypeTag typeTag = aObjectType.getTypeTag();
            return ATypeHierarchy.canPromote(expectedTypeTag, typeTag) ? typeTag : null;
        } else { // union
            unionList = ((AUnionType) aObjectType).getUnionList();
            for (IAType t : unionList) {
                final ATypeTag typeTag = t.getTypeTag();
                if (ATypeHierarchy.canPromote(expectedTypeTag, typeTag)) {
                    return typeTag;
                }
            }
        }
        return null;
    }
    
    private boolean checkType(ATypeTag expectedTypeTag, IAType aObjectType, DataOutput out) throws IOException {
        return getTargetTypeTag(expectedTypeTag, aObjectType) != null;
    }

    private void parseRecord(ARecordType recType, DataOutput out, Boolean datasetRec) throws IOException,
            AsterixException {

        ArrayBackedValueStorage fieldValueBuffer = getTempBuffer();
        ArrayBackedValueStorage fieldNameBuffer = getTempBuffer();
        IARecordBuilder recBuilder = getRecordBuilder();

        // Boolean[] nulls = null;
        BitSet nulls = null;
        if (datasetRec) {
            if (recType != null) {
                nulls = new BitSet(recType.getFieldNames().length);
                recBuilder.reset(recType);
            } else
                recBuilder.reset(null);
        } else if (recType != null) {
            nulls = new BitSet(recType.getFieldNames().length);
            recBuilder.reset(recType);
        } else
            recBuilder.reset(null);

        recBuilder.init();
        int token;
        boolean inRecord = true;
        boolean expectingRecordField = false;
        boolean first = true;

        Boolean openRecordField = false;
        int fieldId = 0;
        IAType fieldType = null;
        do {
            token = nextToken();
            switch (token) {
                case AdmLexer.TOKEN_END_RECORD: {
                    if (expectingRecordField) {
                        throw new AsterixException("Found END_RECORD while expecting a record field.");
                    }
                    inRecord = false;
                    break;
                }
                case AdmLexer.TOKEN_STRING_LITERAL: {
                    // we've read the name of the field
                    // now read the content
                    fieldNameBuffer.reset();
                    fieldValueBuffer.reset();
                    expectingRecordField = false;

                    if (recType != null) {
                        String fldName = admLexer.getLastTokenImage().substring(1,
                                admLexer.getLastTokenImage().length() - 1);
                        fieldId = recBuilder.getFieldId(fldName);
                        if (fieldId < 0 && !recType.isOpen()) {
                            throw new AsterixException("This record is closed, you can not add extra fields !!");
                        } else if (fieldId < 0 && recType.isOpen()) {
                            aStringFieldName.setValue(admLexer.getLastTokenImage().substring(1,
                                    admLexer.getLastTokenImage().length() - 1));
                            stringSerde.serialize(aStringFieldName, fieldNameBuffer.getDataOutput());
                            openRecordField = true;
                            fieldType = null;
                        } else {
                            // a closed field
                            nulls.set(fieldId);
                            fieldType = recType.getFieldTypes()[fieldId];
                            openRecordField = false;
                        }
                    } else {
                        aStringFieldName.setValue(admLexer.getLastTokenImage().substring(1,
                                admLexer.getLastTokenImage().length() - 1));
                        stringSerde.serialize(aStringFieldName, fieldNameBuffer.getDataOutput());
                        openRecordField = true;
                        fieldType = null;
                    }

                    token = nextToken();
                    if (token != AdmLexer.TOKEN_COLON) {
                        throw new AsterixException("Unexpected ADM token kind: " + AdmLexer.tokenKindToString(token)
                                + " while expecting \":\".");
                    }

                    token = nextToken();
                    this.admFromLexerStream(token, fieldType, fieldValueBuffer.getDataOutput(), false);
                    if (openRecordField) {
                        if (fieldValueBuffer.getByteArray()[0] != ATypeTag.NULL.serialize())
                            recBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                    } else if (recType.getFieldTypes()[fieldId].getTypeTag() == ATypeTag.UNION) {
                        if (NonTaggedFormatUtil.isOptionalField((AUnionType) recType.getFieldTypes()[fieldId])) {
                            if (fieldValueBuffer.getByteArray()[0] != ATypeTag.NULL.serialize()) {
                                recBuilder.addField(fieldId, fieldValueBuffer);
                            }
                        }
                    } else {
                        recBuilder.addField(fieldId, fieldValueBuffer);
                    }

                    break;
                }
                case AdmLexer.TOKEN_COMMA: {
                    if (first) {
                        throw new AsterixException("Found COMMA before any record field.");
                    }
                    if (expectingRecordField) {
                        throw new AsterixException("Found COMMA while expecting a record field.");
                    }
                    expectingRecordField = true;
                    break;
                }
                default: {
                    throw new AsterixException("Unexpected ADM token kind: " + AdmLexer.tokenKindToString(token)
                            + " while parsing record fields.");
                }
            }
            first = false;
        } while (inRecord);

        if (recType != null) {
            nullableFieldId = checkNullConstraints(recType, nulls);
            if (nullableFieldId != -1)
                throw new AsterixException("Field " + nullableFieldId + " can not be null");
        }
        recBuilder.write(out, true);
        returnRecordBuilder(recBuilder);
        returnTempBuffer(fieldNameBuffer);
        returnTempBuffer(fieldValueBuffer);
    }

    private int checkNullConstraints(ARecordType recType, BitSet nulls) {

        boolean isNull = false;
        for (int i = 0; i < recType.getFieldTypes().length; i++)
            if (nulls.get(i) == false) {
                IAType type = recType.getFieldTypes()[i];
                if (type.getTypeTag() != ATypeTag.NULL && type.getTypeTag() != ATypeTag.UNION)
                    return i;

                if (type.getTypeTag() == ATypeTag.UNION) { // union
                    unionList = ((AUnionType) type).getUnionList();
                    for (int j = 0; j < unionList.size(); j++)
                        if (unionList.get(j).getTypeTag() == ATypeTag.NULL) {
                            isNull = true;
                            break;
                        }
                    if (!isNull)
                        return i;
                }
            }
        return -1;
    }

    private void parseOrderedList(AOrderedListType oltype, DataOutput out) throws IOException, AsterixException {

        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        OrderedListBuilder orderedListBuilder = (OrderedListBuilder) getOrderedListBuilder();

        IAType itemType = null;
        if (oltype != null)
            itemType = oltype.getItemType();
        orderedListBuilder.reset(oltype);

        int token;
        boolean inList = true;
        boolean expectingListItem = false;
        boolean first = true;
        do {
            token = nextToken();
            if (token == AdmLexer.TOKEN_END_ORDERED_LIST) {
                if (expectingListItem) {
                    throw new AsterixException("Found END_COLLECTION while expecting a list item.");
                }
                inList = false;
            } else if (token == AdmLexer.TOKEN_COMMA) {
                if (first) {
                    throw new AsterixException("Found COMMA before any list item.");
                }
                if (expectingListItem) {
                    throw new AsterixException("Found COMMA while expecting a list item.");
                }
                expectingListItem = true;
            } else {
                expectingListItem = false;
                itemBuffer.reset();

                admFromLexerStream(token, itemType, itemBuffer.getDataOutput(), false);
                orderedListBuilder.addItem(itemBuffer);
            }
            first = false;
        } while (inList);
        orderedListBuilder.write(out, true);
        returnOrderedListBuilder(orderedListBuilder);
        returnTempBuffer(itemBuffer);
    }

    private void parseUnorderedList(AUnorderedListType uoltype, DataOutput out) throws IOException, AsterixException {

        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        UnorderedListBuilder unorderedListBuilder = (UnorderedListBuilder) getUnorderedListBuilder();

        IAType itemType = null;

        if (uoltype != null)
            itemType = uoltype.getItemType();
        unorderedListBuilder.reset(uoltype);

        int token;
        boolean inList = true;
        boolean expectingListItem = false;
        boolean first = true;
        do {
            token = nextToken();
            if (token == AdmLexer.TOKEN_END_RECORD) {
                if (nextToken() == AdmLexer.TOKEN_END_RECORD) {
                    if (expectingListItem) {
                        throw new AsterixException("Found END_COLLECTION while expecting a list item.");
                    } else {
                        inList = false;
                    }
                } else {
                    throw new AsterixException("Found END_RECORD while expecting a list item.");
                }
            } else if (token == AdmLexer.TOKEN_COMMA) {
                if (first) {
                    throw new AsterixException("Found COMMA before any list item.");
                }
                if (expectingListItem) {
                    throw new AsterixException("Found COMMA while expecting a list item.");
                }
                expectingListItem = true;
            } else {
                expectingListItem = false;
                itemBuffer.reset();
                admFromLexerStream(token, itemType, itemBuffer.getDataOutput(), false);
                unorderedListBuilder.addItem(itemBuffer);
            }
            first = false;
        } while (inList);
        unorderedListBuilder.write(out, true);
        returnUnorderedListBuilder(unorderedListBuilder);
        returnTempBuffer(itemBuffer);
    }

    private int nextToken() throws AsterixException {
        try {
            return admLexer.next();
        } catch (AdmLexerException e) {
            throw new AsterixException(e);
        } catch (IOException e) {
            throw new AsterixException(e);
        }
    }

    private IARecordBuilder getRecordBuilder() {
        RecordBuilder recBuilder = (RecordBuilder) recordBuilderPool.poll();
        if (recBuilder != null)
            return recBuilder;
        else
            return new RecordBuilder();
    }

    private void returnRecordBuilder(IARecordBuilder recBuilder) {
        this.recordBuilderPool.add(recBuilder);
    }

    private IAsterixListBuilder getOrderedListBuilder() {
        OrderedListBuilder orderedListBuilder = (OrderedListBuilder) orderedListBuilderPool.poll();
        if (orderedListBuilder != null)
            return orderedListBuilder;
        else
            return new OrderedListBuilder();
    }

    private void returnOrderedListBuilder(IAsterixListBuilder orderedListBuilder) {
        this.orderedListBuilderPool.add(orderedListBuilder);
    }

    private IAsterixListBuilder getUnorderedListBuilder() {
        UnorderedListBuilder unorderedListBuilder = (UnorderedListBuilder) unorderedListBuilderPool.poll();
        if (unorderedListBuilder != null)
            return unorderedListBuilder;
        else
            return new UnorderedListBuilder();
    }

    private void returnUnorderedListBuilder(IAsterixListBuilder unorderedListBuilder) {
        this.unorderedListBuilderPool.add(unorderedListBuilder);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        ArrayBackedValueStorage tmpBaaos = baaosPool.poll();
        if (tmpBaaos != null) {
            return tmpBaaos;
        } else {
            return new ArrayBackedValueStorage();
        }
    }

    private void returnTempBuffer(ArrayBackedValueStorage tempBaaos) {
        baaosPool.add(tempBaaos);
    }


    private void parseToNumericTarget(ATypeTag typeTag, IAType objectType, DataOutput out) throws AsterixException,
            IOException {
        final ATypeTag targetTypeTag = getTargetTypeTag(typeTag, objectType);
        if (targetTypeTag == null || !parseValue(admLexer.getLastTokenImage(), targetTypeTag, out)) {
            throw new AsterixException(mismatchErrorMessage + objectType.getTypeName() + mismatchErrorMessage2
                    + typeTag);
        }
    }
    
    private void parseAndCastNumeric(ATypeTag typeTag, IAType objectType, DataOutput out) throws AsterixException, IOException {
        final ATypeTag targetTypeTag = getTargetTypeTag(typeTag, objectType);
        DataOutput dataOutput = out;
        if (targetTypeTag != typeTag) {
            castBuffer.reset();
            dataOutput = castBuffer.getDataOutput();
        }

        if (targetTypeTag == null || !parseValue(admLexer.getLastTokenImage(), typeTag, dataOutput)) {
            throw new AsterixException(mismatchErrorMessage + objectType.getTypeName() + mismatchErrorMessage2
                    + typeTag);
        }

        if (targetTypeTag != typeTag) {
            ITypePromoteComputer promoteComputer = ATypeHierarchy.getTypePromoteComputer(typeTag, targetTypeTag);
            // the availability if the promote computer should be consistent with the availability of a target type
            assert promoteComputer != null;
            // do the promotion; note that the type tag field should be skipped
            promoteComputer.promote(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                    castBuffer.getLength() - 1, out);
        }
    }
    
    private void parseConstructor(ATypeTag typeTag, IAType objectType, DataOutput out) throws AsterixException {
        try {
            final ATypeTag targetTypeTag = getTargetTypeTag(typeTag, objectType);
            if (targetTypeTag != null) {
                DataOutput dataOutput = out;
                if (targetTypeTag != typeTag) {
                    castBuffer.reset();
                    dataOutput = castBuffer.getDataOutput();
                }
                int token = admLexer.next();
                if (token == AdmLexer.TOKEN_CONSTRUCTOR_OPEN) {
                    token = admLexer.next();
                    if (token == AdmLexer.TOKEN_STRING_LITERAL) {
                        final String unquoted = admLexer.getLastTokenImage().substring(1,
                                admLexer.getLastTokenImage().length() - 1);
                        if (!parseValue(unquoted, typeTag, dataOutput)) {
                            throw new AsterixException("Missing deserializer method for constructor: "
                                    + AdmLexer.tokenKindToString(token) + ".");
                        }
                        token = admLexer.next();
                        if (token == AdmLexer.TOKEN_CONSTRUCTOR_CLOSE) {
                            if (targetTypeTag != typeTag) {
                                ITypePromoteComputer promoteComputer = ATypeHierarchy.getTypePromoteComputer(typeTag, targetTypeTag);
                                // the availability if the promote computer should be consistent with the availability of a target type
                                assert promoteComputer != null;
                                // do the promotion; note that the type tag field should be skipped
                                promoteComputer.promote(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                                        castBuffer.getLength() - 1, out);
                            }
                            return;
                        }
                    }
                }
            }
        } catch (IOException | AdmLexerException e) {
            throw new AsterixException(e);
        }
        throw new AsterixException(mismatchErrorMessage + objectType.getTypeName() + ". Got " + typeTag + " instead.");
    }

    private boolean parseValue(final String unquoted, ATypeTag typeTag, DataOutput out)
            throws AsterixException, HyracksDataException, IOException {
        switch (typeTag) {
            case BOOLEAN:
                parseBoolean(unquoted, out);
                return true;
            case INT8:
                parseInt8(unquoted, out);
                return true;
            case INT16:
                parseInt16(unquoted, out);
                return true;
            case INT32:
                parseInt32(unquoted, out);
                return true;
            case INT64:
                parseInt64(unquoted, out);
                return true;
            case FLOAT:
                aFloat.setValue(Float.parseFloat(unquoted));
                floatSerde.serialize(aFloat, out);
                return true;
            case DOUBLE:
                aDouble.setValue(Double.parseDouble(unquoted));
                doubleSerde.serialize(aDouble, out);
                return true;
            case STRING:
                aString.setValue(unquoted);
                stringSerde.serialize(aString, out);
                return true;
            case TIME:
                parseTime(unquoted, out);
                return true;
            case DATE:
                parseDate(unquoted, out);
                return true;
            case DATETIME:
                parseDatetime(unquoted, out);
                return true;
            case DURATION:
                parseDuration(unquoted, out);
                return true;
            case DAYTIMEDURATION:
                parseDayTimeDuration(unquoted, out);
                return true;
            case YEARMONTHDURATION:
                parseYearMonthDuration(unquoted, out);
                return true;
            case POINT:
                parsePoint(unquoted, out);
                return true;
            case POINT3D:
                parsePoint3d(unquoted, out);
                return true;
            case CIRCLE:
                parseCircle(unquoted, out);
                return true;
            case RECTANGLE:
                parseRectangle(unquoted, out);
                return true;
            case LINE:
                parseLine(unquoted, out);
                return true;
            case POLYGON:
                parsePolygon(unquoted, out);
                return true;
            default:
                return false;
        }
    }

    private void parseBoolean(String bool, DataOutput out) throws AsterixException {
        String errorMessage = "This can not be an instance of boolean";
        try {
            if (bool.equals("true"))
                booleanSerde.serialize(ABoolean.TRUE, out);
            else if (bool.equals("false"))
                booleanSerde.serialize(ABoolean.FALSE, out);
            else
                throw new AsterixException(errorMessage);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parseInt8(String int8, DataOutput out) throws AsterixException {
        String errorMessage = "This can not be an instance of int8";
        try {
            boolean positive = true;
            byte value = 0;
            int offset = 0;

            if (int8.charAt(offset) == '+')
                offset++;
            else if (int8.charAt(offset) == '-') {
                offset++;
                positive = false;
            }
            for (; offset < int8.length(); offset++) {
                if (int8.charAt(offset) >= '0' && int8.charAt(offset) <= '9')
                    value = (byte) (value * 10 + int8.charAt(offset) - '0');
                else if (int8.charAt(offset) == 'i' && int8.charAt(offset + 1) == '8' && offset + 2 == int8.length())
                    break;
                else
                    throw new AsterixException(errorMessage);
            }
            if (value < 0)
                throw new AsterixException(errorMessage);
            if (value > 0 && !positive)
                value *= -1;
            aInt8.setValue(value);
            int8Serde.serialize(aInt8, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parseInt16(String int16, DataOutput out) throws AsterixException {
        String errorMessage = "This can not be an instance of int16";
        try {
            boolean positive = true;
            short value = 0;
            int offset = 0;

            if (int16.charAt(offset) == '+')
                offset++;
            else if (int16.charAt(offset) == '-') {
                offset++;
                positive = false;
            }
            for (; offset < int16.length(); offset++) {
                if (int16.charAt(offset) >= '0' && int16.charAt(offset) <= '9')
                    value = (short) (value * 10 + int16.charAt(offset) - '0');
                else if (int16.charAt(offset) == 'i' && int16.charAt(offset + 1) == '1'
                        && int16.charAt(offset + 2) == '6' && offset + 3 == int16.length())
                    break;
                else
                    throw new AsterixException(errorMessage);
            }
            if (value < 0)
                throw new AsterixException(errorMessage);
            if (value > 0 && !positive)
                value *= -1;
            aInt16.setValue(value);
            int16Serde.serialize(aInt16, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parseInt32(String int32, DataOutput out) throws AsterixException {

        String errorMessage = "This can not be an instance of int32";
        try {
            boolean positive = true;
            int value = 0;
            int offset = 0;

            if (int32.charAt(offset) == '+')
                offset++;
            else if (int32.charAt(offset) == '-') {
                offset++;
                positive = false;
            }
            for (; offset < int32.length(); offset++) {
                if (int32.charAt(offset) >= '0' && int32.charAt(offset) <= '9')
                    value = (value * 10 + int32.charAt(offset) - '0');
                else if (int32.charAt(offset) == 'i' && int32.charAt(offset + 1) == '3'
                        && int32.charAt(offset + 2) == '2' && offset + 3 == int32.length())
                    break;
                else
                    throw new AsterixException(errorMessage);
            }
            if (value < 0)
                throw new AsterixException(errorMessage);
            if (value > 0 && !positive)
                value *= -1;

            aInt32.setValue(value);
            int32Serde.serialize(aInt32, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parseInt64(String int64, DataOutput out) throws AsterixException {
        String errorMessage = "This can not be an instance of int64";
        try {
            boolean positive = true;
            long value = 0;
            int offset = 0;

            if (int64.charAt(offset) == '+')
                offset++;
            else if (int64.charAt(offset) == '-') {
                offset++;
                positive = false;
            }
            for (; offset < int64.length(); offset++) {
                if (int64.charAt(offset) >= '0' && int64.charAt(offset) <= '9')
                    value = (value * 10 + int64.charAt(offset) - '0');
                else if (int64.charAt(offset) == 'i' && int64.charAt(offset + 1) == '6'
                        && int64.charAt(offset + 2) == '4' && offset + 3 == int64.length())
                    break;
                else
                    throw new AsterixException(errorMessage);
            }
            if (value < 0)
                throw new AsterixException(errorMessage);
            if (value > 0 && !positive)
                value *= -1;

            aInt64.setValue(value);
            int64Serde.serialize(aInt64, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parsePoint(String point, DataOutput out) throws AsterixException {
        try {
            APointSerializerDeserializer.parse(point, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parsePoint3d(String point3d, DataOutput out) throws AsterixException {
        try {
            APoint3DSerializerDeserializer.parse(point3d, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseCircle(String circle, DataOutput out) throws AsterixException {
        try {
            ACircleSerializerDeserializer.parse(circle, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseRectangle(String rectangle, DataOutput out) throws AsterixException {
        try {
            ARectangleSerializerDeserializer.parse(rectangle, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseLine(String line, DataOutput out) throws AsterixException {
        try {
            ALineSerializerDeserializer.parse(line, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parsePolygon(String polygon, DataOutput out) throws AsterixException, IOException {
        try {
            APolygonSerializerDeserializer.parse(polygon, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseTime(String time, DataOutput out) throws AsterixException {
        try {
            ATimeSerializerDeserializer.parse(time, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseDate(String date, DataOutput out) throws AsterixException, IOException {
        try {
            ADateSerializerDeserializer.parse(date, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

}
