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

import { flatbuffers } from 'flatbuffers';
import { Vector } from '../vector/vector';
import { TypedArray, TypedArrayConstructor } from '../vector/types';
import {
    BinaryVector, BoolVector, Date32Vector, Date64Vector, Time32Vector,
    Time64Vector, TimestampVector, Utf8Vector, Int8Vector, Int16Vector,
    Int32Vector, Int64Vector, Uint8Vector, Uint16Vector, Uint32Vector,
    Uint64Vector, Float32Vector, Float64Vector, DecimalVector, ListVector,
    StructVector } from '../vector/arrow';

import * as Schema_ from '../format/fb/Schema'; import Type
= Schema_.org.apache.arrow.flatbuf.Type; import { FieldBuilder,
    FieldNodeBuilder } from '../format/arrow';

import { Int64, Int128 } from '../util/int';
import { TextEncoder } from 'text-encoding-utf-8';
const encoder = new TextEncoder('utf-8');

export function* readJSON(obj: any): IterableIterator<Vector<any>[]> {
    let schema: any = {};
    for (const field of obj.schema.fields) {
        schema[field.name] = field;
    }

    for (const batch of obj.batches) {
        yield batch.columns.map((column: any): Vector => readVector(schema[column.name], column));
    }
}

function readVector(field: any, column: any): Vector {
    return readDictionaryVector(field, column) || readValueVector(field, column);
}

function readDictionaryVector(field: any, column: any) {
    if (field.name == column.name) { return null; } else { return null; }
}

function readValueVector(field: any, column: any): Vector {
    switch (field.type.name) {
        //case "NONE": return readNullVector(field, column);
        //case "null": return readNullVector(field, column);
        //case "map": return readMapVector(field, column);
        case 'int': return readIntVector(field, column);
        case 'bool': return readBoolVector(field, column);
        case "date": return readDateVector(field, column);
        case 'list': return readListVector(field, column);
        case 'utf8': return readUtf8Vector(field, column);
        case "time": return readTimeVector(field, column);
        //case "union": return readUnionVector(field, column);
        case 'binary': return readBinaryVector(field, column);
        case 'decimal': return readDecimalVector(field, column);
        case 'struct': return readStructVector(field, column);
        case 'floatingpoint': return readFloatVector(field, column);
        case "timestamp": return readTimestampVector(field, column);
        //case "fixedsizelist": return readFixedSizeListVector(field, column);
        //case "fixedsizebinary": return readFixedSizeBinaryVector(field, column);
    }
    throw new Error(`Unrecognized Vector { name: ${field.name}, type: ${field.type.name} }`);
}

function readIntVector(field: any, column: any) {
    if (field.type.isSigned) {
        switch (field.type.bitWidth) {
            case  8: return new  Int8Vector(readNumeric(field, column, Int8Array));
            case 16: return new Int16Vector(readNumeric(field, column, Int16Array));
            case 32: return new Int32Vector(readNumeric(field, column, Int32Array));
            case 64: return new Int64Vector(readInt64(field, column, Int32Array));
        }
    }
    switch (field.type.bitWidth) {
        case  8: return new  Uint8Vector(readNumeric(field, column, Uint8Array));
        case 16: return new Uint16Vector(readNumeric(field, column, Uint16Array));
        case 32: return new Uint32Vector(readNumeric(field, column, Uint32Array));
        case 64: return new Uint64Vector(readInt64(field, column, Uint32Array));
    }
    throw new Error(`Unrecognized Int { isSigned: ${field.type.isSigned}, bitWidth: ${field.type.bitWidth} }`);
}

function readBoolVector(fieldObj: any, column: any) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    const data = readBoolean(column.DATA);
    return new BoolVector({field, fieldNode, validity, data});
}

function readListVector(fieldObj: any, column: any): Vector {
    const { field, fieldNode, validity, offsets } = readList(fieldObj, column);
    return new ListVector({
        field, fieldNode, validity, offsets,
        values: readVector(fieldObj.children[0], column.children[0])
    });
}

function readUtf8Vector(fieldObj: any, column: any): Vector {
    const { field, fieldNode, validity, offsets } = readList(fieldObj, column);
    const data = encoder.encode(column.DATA.join(''));
    return new Utf8Vector({
        field, fieldNode,
        values: new BinaryVector({
            validity, offsets, data
        })
    });
}

function readDateVector(field: any, state: any) {
    const type = field.type!;
    switch (type.unit) {
        case "DAY": return new Date32Vector({ ...readNumeric(field, state, Int32Array), unit: type.unit });
        case "MILLISECOND": return new Date64Vector({ ...readInt64(field, state, Int32Array), unit: type.unit });
    }
    throw new Error(`Unrecognized Date { unit: ${type.unit} }`);
}

function readTimeVector(field: any, state: any) {
    const type = field.type!;
    switch (type.bitWidth) {
        case 32: return new Time32Vector({ ...readNumeric(field, state, Int32Array), unit:type.unit });
        case 64: return new Time64Vector({ ...readInt64(field, state, Uint32Array), unit: type.unit });
    }
    throw new Error(`Unrecognized Time { unit: ${type.unit}, bitWidth: ${type.bitWidth} }`);
}

function readTimestampVector(field: any, state: any) {
    const type = field.type!;
    return new TimestampVector({
        ... readInt64(field, state, Uint32Array),
        timezone: type.timezone!,
        unit: type.unit!,
    });
}

function readBinaryVector(field: any, column: any) {
    return new BinaryVector(readBinary(field, column));
}

function readDecimalVector(fieldObj: any, column: any) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);

    let data = new Uint32Array(column.DATA.length * 4);
    for (let i = 0; i < column.DATA.length; ++i) {
        Int128.fromString(column.DATA[i], new Uint32Array(data.buffer, data.byteOffset + 4 * 4 * i, 4));
    }

    return new DecimalVector({
        scale: fieldObj.scale,
        precision: fieldObj.precision,
        field, fieldNode, validity, data
    });
}

function readStructVector(fieldObj: any, column: any) {
    const n = fieldObj.children.length;
    const columns = new Array<Vector>(n);
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    for (let i = -1; ++i < n;) {
            columns[i] = readVector(fieldObj.children[i], column.children[i]);
    }
    return new StructVector({ field, fieldNode, validity, columns });
}

function readFloatVector(field: any, column: any) {
    switch (field.type.precision) {
        // TODO: case "HALF":   return new Float16Vector(readNumeric(field, column, Uint16Array));
        case 'SINGLE': return new Float32Vector(readNumeric(field, column, Float32Array));
        case 'DOUBLE': return new Float64Vector(readNumeric(field, column, Float64Array));
    }
    throw new Error(`Unrecognized FloatingPoint { precision: ${field.type.precision} }`);
}

function readList(fieldObj: any, column: any) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    const offsets = readData(Int32Array, column.OFFSET);
    return { field, fieldNode, validity, offsets };
}

// "VALIDITY": [1,1],
// "OFFSET": [0,7,14],
// "DATA": ["49BC7D5B6C47D2","3F5FB6D9322026"]
function readBinary(fieldObj: any, column: any) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    const offsets = readData(Int32Array, column.OFFSET);
    // There are definitely more efficient ways to do this... but it gets the
    // job done.
    const joined = column.DATA.join('');
    let data = new Uint8Array(joined.length / 2);
    for (let i = 0; i < joined.length; i += 2) {
        data[i >> 1] = parseInt(joined.substr(i, 2), 16);
    }
    return { field, fieldNode, validity, offsets, data };
}

function readNumeric<T extends TypedArray>(fieldObj: any, column: any, ArrayConstructor: TypedArrayConstructor<T>) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    const data = readData(ArrayConstructor, column.DATA);
    return { field, fieldNode, validity, data };
}

function readInt64<T extends (Uint32Array|Int32Array)>(fieldObj: any, column: any, ArrayConstructor: TypedArrayConstructor<T>) {
    const field = fieldFromJSON(fieldObj);
    const fieldNode = fieldNodeFromJSON(column);
    const validity = readValidity(column);
    let data = new ArrayConstructor(column.DATA.length * 2);
    for (let i = 0; i < column.DATA.length; ++i) {
        // Force all values (even numbers) to be parsed as strings since
        // pulling out high and low bits seems to lose precision sometimes
        // For example:
        //     > -4613034156400212000 >>> 0
        //     721782784
        // The correct lower 32-bits are 721782752
        Int64.fromString(column.DATA[i].toString(), new Uint32Array(data.buffer, data.byteOffset + 2 * i * 4, 2));
    }
    return { field, fieldNode, validity, data };
}

function readData<T extends TypedArray>(ArrayConstructor: TypedArrayConstructor<T>, column: [number]) {
    return new ArrayConstructor(column);
}

function readValidity(column: any) {
    return readBoolean(column.VALIDITY);
}

function readBoolean(arr: Array<number>) {
    let rtrn: Uint8Array = new Uint8Array(Math.ceil(arr.length / 8));
    for (const {item, index} of arr.map((item: any, index: number) => ({item, index}))) {
        rtrn[index / 8 | 0] |= item << (index % 8);
    }
    return rtrn;
}

const TYPE_LOOKUP: {[index: string]: Type} = {
    'NONE':            Type.NONE,
    'null':            Type.Null,
    'map':             Type.Map,
    'int':             Type.Int,
    'bool':            Type.Bool,
    'date':            Type.Date,
    'list':            Type.List,
    'utf8':            Type.Utf8,
    'time':            Type.Time,
    'union':           Type.Union,
    'binary':          Type.Binary,
    'decimal':         Type.Decimal,
    'struct_':         Type.Struct_,
    'floatingpoint':   Type.FloatingPoint,
    'timestamp':       Type.Timestamp,
    'fixedsizelist':   Type.FixedSizeList,
    'fixedsizebinary': Type.FixedSizeBinary
};

function fieldFromJSON(obj: any): FieldBuilder {
    // TODO: metadata
    return new FieldBuilder(obj.name, TYPE_LOOKUP[obj.type.name], obj.nullable, []);
}

function fieldNodeFromJSON(obj: any): FieldNodeBuilder {
    let nullCount = obj.VALIDITY && obj.VALIDITY.length ?
        obj.VALIDITY.reduce((sum: number, current: number) => sum + current) : 0;
    return new FieldNodeBuilder(
        flatbuffers.Long.create(obj.count, 0),
        flatbuffers.Long.create(nullCount, 0)
    );
}
