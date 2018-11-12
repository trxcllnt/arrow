// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import * as Schema_ from '../../fb/Schema';
import { Schema, Field } from '../../schema';
import {
    DataType, Dictionary, TimeBitWidth,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Map_, Struct, Union,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp,
} from '../../type';

import _Int = Schema_.org.apache.arrow.flatbuf.Int;
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import _Field = Schema_.org.apache.arrow.flatbuf.Field;
import _Schema = Schema_.org.apache.arrow.flatbuf.Schema;
import _DictionaryEncoding = Schema_.org.apache.arrow.flatbuf.DictionaryEncoding;

Field.decode = decodeField;
Schema.decode = decodeSchema;

declare module '../../schema' {
    namespace Field { export { decodeField as decode }; }
    namespace Schema { export { decodeSchema as decode }; }
}

function decodeSchema(_schema: _Schema, dictionaryTypes: Map<number, DataType>) {
    const fields = decodeSchemaFields(_schema, dictionaryTypes);
    return new Schema(fields, decodeCustomMetadata(_schema), dictionaryTypes);
}

function decodeSchemaFields(schema: _Schema, dictionaryTypes: Map<number, DataType> | null) {
    return Array.from(
        { length: schema.fieldsLength() },
        (_, i) => schema.fields(i)!
    ).filter(Boolean).map((f) => Field.decode(f, dictionaryTypes));
}

function decodeFieldChildren(field: _Field, dictionaryTypes: Map<number, DataType> | null): Field[] {
    return Array.from(
        { length: field.childrenLength() },
        (_, i) => field.children(i)!
    ).filter(Boolean).map((f) => Field.decode(f, dictionaryTypes));
}

function decodeField(f: _Field, dictionaryTypes: Map<number, DataType> | null) {

    let id: number;
    let field: Field | void;
    let keys: _Int | Int | null;
    let type: DataType<any> | null;
    let dict: _DictionaryEncoding | null;

    // If no dictionary encoding, or in the process of decoding the children of a dictionary-encoded field
    if (!dictionaryTypes || !(dict = f.dictionary())) {
        type = decodeFieldType(f, decodeFieldChildren(f, dictionaryTypes));
        field = new Field(f.name()!, type, f.nullable(), decodeCustomMetadata(f));
    }
    // tslint:disable
    // If dictionary encoded and the first time we've seen this dictionary id, decode
    // the data type and child fields, then wrap in a Dictionary type and insert the
    // data type into the dictionary types map.
    else if (!dictionaryTypes.has(id = dict.id().low)) {
        // a dictionary index defaults to signed 32 bit int if unspecified
        keys = (keys = dict.indexType()) ? decodeIntType(keys) : new Int(true, 32);
        type = new Dictionary(decodeFieldType(f, decodeFieldChildren(f, null)), keys, id, dict.isOrdered());
        field = new Field(f.name()!, type, f.nullable(), decodeCustomMetadata(f));
        dictionaryTypes.set(id, (type as Dictionary).dictionary);
    }
    // If dictionary encoded, and have already seen this dictionary Id in the schema, then reuse the
    // data type and wrap in a new Dictionary type and field.
    else {
        // a dictionary index defaults to signed 32 bit int if unspecified
        keys = (keys = dict.indexType()) ? decodeIntType(keys) : new Int(true, 32);
        type = new Dictionary(dictionaryTypes.get(id)!, keys, id, dict.isOrdered());
        field = new Field(f.name()!, type, f.nullable(), decodeCustomMetadata(f));
    }
    return field || null;
}

function decodeCustomMetadata(parent?: _Schema | _Field | null) {
    const data = new Map<string, string>();
    if (parent) {
        for (let entry, key, i = -1, n = parent.customMetadataLength() | 0; ++i < n;) {
            if ((entry = parent.customMetadata(i)) && (key = entry.key()) != null) {
                data.set(key, entry.value()!);
            }
        }
    }
    return data;
}

function decodeIntType(_type: _Int) {
    return new Int(_type.isSigned(), _type.bitWidth());
}

function decodeFieldType(f: _Field, children?: Field[]): DataType<any> {

    const typeId = f.typeType();

    switch (typeId) {
        case Type.NONE:    return new DataType();
        case Type.Null:    return new Null();
        case Type.Binary:  return new Binary();
        case Type.Utf8:    return new Utf8();
        case Type.Bool:    return new Bool();
        case Type.List:    return new List(children || []);
        case Type.Struct_: return new Struct(children || []);
    }

    switch (typeId) {
        case Type.Int: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Int())!;
            return new Int(t.isSigned(), t.bitWidth());
        }
        case Type.FloatingPoint: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.FloatingPoint())!;
            return new Float(t.precision());
        }
        case Type.Decimal: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Decimal())!;
            return new Decimal(t.scale(), t.precision());
        }
        case Type.Date: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Date())!;
            return new Date_(t.unit());
        }
        case Type.Time: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Time())!;
            return new Time(t.unit(), t.bitWidth() as TimeBitWidth);
        }
        case Type.Timestamp: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Timestamp())!;
            return new Timestamp(t.unit(), t.timezone());
        }
        case Type.Interval: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Interval())!;
            return new Interval(t.unit());
        }
        case Type.Union: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Union())!;
            return new Union(t.mode(), (t.typeIdsArray() || []) as Type[], children || []);
        }
        case Type.FixedSizeBinary: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.FixedSizeBinary())!;
            return new FixedSizeBinary(t.byteWidth());
        }
        case Type.FixedSizeList: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.FixedSizeList())!;
            return new FixedSizeList(t.listSize(), children || []);
        }
        case Type.Map: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Map())!;
            return new Map_(children || [], t.keysSorted());
        }
    }
    throw new Error(`Unrecognized type: "${Type[typeId]}" (${typeId})`);
}
