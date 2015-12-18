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
package org.apache.asterix.lang.common.expression;

import java.util.ArrayList;

import org.apache.asterix.common.annotations.IRecordFieldDataGen;
import org.apache.asterix.common.annotations.UndeclaredFieldsDataGen;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class RecordTypeDefinition extends TypeExpression {

    public enum RecordKind {
        OPEN,
        CLOSED
    }

    private ArrayList<String> fieldNames;
    private ArrayList<NullableTypeExpression> fieldTypes;
    private ArrayList<IRecordFieldDataGen> fieldDataGen;
    private RecordKind recordKind;
    private UndeclaredFieldsDataGen undeclaredFieldsDataGen;

    public RecordTypeDefinition() {
        fieldNames = new ArrayList<String>();
        fieldTypes = new ArrayList<NullableTypeExpression>();
        fieldDataGen = new ArrayList<IRecordFieldDataGen>();
    }

    @Override
    public TypeExprKind getTypeKind() {
        return TypeExprKind.RECORD;
    }

    public void addField(String name, NullableTypeExpression type, IRecordFieldDataGen fldDataGen) {
        addField(name, type);
        fieldDataGen.add(fldDataGen);
    }

    public void addField(String name, NullableTypeExpression type) {
        fieldNames.add(name);
        fieldTypes.add(type);
    }

    public ArrayList<String> getFieldNames() {
        return fieldNames;
    }

    public ArrayList<NullableTypeExpression> getFieldTypes() {
        return fieldTypes;
    }

    public ArrayList<IRecordFieldDataGen> getFieldDataGen() {
        return fieldDataGen;
    }

    public RecordKind getRecordKind() {
        return recordKind;
    }

    public void setRecordKind(RecordKind recordKind) {
        this.recordKind = recordKind;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    public void setUndeclaredFieldsDataGen(UndeclaredFieldsDataGen undeclaredFieldsDataGen) {
        this.undeclaredFieldsDataGen = undeclaredFieldsDataGen;
    }

    public UndeclaredFieldsDataGen getUndeclaredFieldsDataGen() {
        return undeclaredFieldsDataGen;
    }

}
