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

import { Data } from '../data';
import { Visitor } from '../visitor';
import { Vector } from '../interfaces';
import { TextDecoder } from 'text-encoding-utf-8';
import { Type, UnionMode, Precision, DateUnit, TimeUnit, IntervalUnit } from '../enum';
import {
    DataType, Dictionary,
    Bool, Null, Utf8, Binary, Decimal, FixedSizeBinary, List, FixedSizeList, Map_, Struct,
    Float, Float16, Float32, Float64,
    Int, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64,
    Date_, DateDay, DateMillisecond,
    Interval, IntervalDayTime, IntervalYearMonth,
    Time, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    Timestamp, TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Union, DenseUnion, SparseUnion, 
} from '../type';

export const decodeUtf8 = ((decoder) =>
    decoder.decode.bind(decoder) as (input?: ArrayBufferLike | ArrayBufferView) => string
)(new TextDecoder('utf-8'));

export const epochSecondsToMs = (data: Int32Array, index: number) => 1000 * data[index];
export const epochDaysToMs = (data: Int32Array, index: number) => 86400000 * data[index];
export const epochMillisecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1]) + (data[index] >>> 0);
export const epochMicrosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000) + ((data[index] >>> 0) / 1000);
export const epochNanosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000000) + ((data[index] >>> 0) / 1000000);

export const epochMillisecondsToDate = (epochMs: number) => new Date(epochMs);
export const epochDaysToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochDaysToMs(data, index));
export const epochSecondsToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochSecondsToMs(data, index));
export const epochNanosecondsLongToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochNanosecondsLongToMs(data, index));
export const epochMillisecondsLongToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochMillisecondsLongToMs(data, index));

export interface GetVisitor extends Visitor {
    visitMany <T extends Vector>  (nodes: T[])                                                     : T['TValue'][];
    visit     <T extends Vector>  (node: T, ...args: any[])                                        : T['TValue'];
    getVisitFn<T extends Type>    (node: T)         : (vector: Vector<T>, ...args: any[]) => Vector<T>['TValue'];
    getVisitFn<T extends DataType>(node: Vector<T>) : (vector: Vector<T>, ...args: any[]) => Vector<T>['TValue'];
    getVisitFn<T extends DataType>(node: Data<T>)   : (vector: Vector<T>, ...args: any[]) => Vector<T>['TValue'];
    getVisitFn<T extends DataType>(node: T)         : (vector: Vector<T>, ...args: any[]) => Vector<T>['TValue'];
}

export class GetVisitor extends Visitor {
    public visitNull                 <T extends Null>                (vector: Vector<T>, index: number) { return                  getNull(vector, index); }
    public visitBool                 <T extends Bool>                (vector: Vector<T>, index: number) { return                  getBool(vector, index); }
    public visitInt                  <T extends Int>                 (vector: Vector<T>, index: number) { return                   getInt(vector, index); }
    public visitInt8                 <T extends Int8>                (vector: Vector<T>, index: number) { return               getNumeric(vector, index); }
    public visitInt16                <T extends Int16>               (vector: Vector<T>, index: number) { return               getNumeric(vector, index); }
    public visitInt32                <T extends Int32>               (vector: Vector<T>, index: number) { return               getNumeric(vector, index); }
    public visitInt64                <T extends Int64>               (vector: Vector<T>, index: number) { return             getNumericX2(vector, index); }
    public visitUint8                <T extends Uint8>               (vector: Vector<T>, index: number) { return               getNumeric(vector, index); }
    public visitUint16               <T extends Uint16>              (vector: Vector<T>, index: number) { return               getNumeric(vector, index); }
    public visitUint32               <T extends Uint32>              (vector: Vector<T>, index: number) { return               getNumeric(vector, index); }
    public visitUint64               <T extends Uint64>              (vector: Vector<T>, index: number) { return             getNumericX2(vector, index); }
    public visitFloat                <T extends Float>               (vector: Vector<T>, index: number) { return                 getFloat(vector, index); }
    public visitFloat16              <T extends Float16>             (vector: Vector<T>, index: number) { return               getFloat16(vector, index); }
    public visitFloat32              <T extends Float32>             (vector: Vector<T>, index: number) { return               getNumeric(vector, index); }
    public visitFloat64              <T extends Float64>             (vector: Vector<T>, index: number) { return               getNumeric(vector, index); }
    public visitUtf8                 <T extends Utf8>                (vector: Vector<T>, index: number) { return                  getUtf8(vector, index); }
    public visitBinary               <T extends Binary>              (vector: Vector<T>, index: number) { return                getBinary(vector, index); }
    public visitFixedSizeBinary      <T extends FixedSizeBinary>     (vector: Vector<T>, index: number) { return       getFixedSizeBinary(vector, index); }
    public visitDate                 <T extends Date_>               (vector: Vector<T>, index: number) { return                  getDate(vector, index); }
    public visitDateDay              <T extends DateDay>             (vector: Vector<T>, index: number) { return               getDateDay(vector, index); }
    public visitDateMillisecond      <T extends DateMillisecond>     (vector: Vector<T>, index: number) { return       getDateMillisecond(vector, index); }
    public visitTimestamp            <T extends Timestamp>           (vector: Vector<T>, index: number) { return             getTimestamp(vector, index); }
    public visitTimestampSecond      <T extends TimestampSecond>     (vector: Vector<T>, index: number) { return       getTimestampSecond(vector, index); }
    public visitTimestampMillisecond <T extends TimestampMillisecond>(vector: Vector<T>, index: number) { return  getTimestampMillisecond(vector, index); }
    public visitTimestampMicrosecond <T extends TimestampMicrosecond>(vector: Vector<T>, index: number) { return  getTimestampMicrosecond(vector, index); }
    public visitTimestampNanosecond  <T extends TimestampNanosecond> (vector: Vector<T>, index: number) { return   getTimestampNanosecond(vector, index); }
    public visitTime                 <T extends Time>                (vector: Vector<T>, index: number) { return                  getTime(vector, index); }
    public visitTimeSecond           <T extends TimeSecond>          (vector: Vector<T>, index: number) { return            getTimeSecond(vector, index); }
    public visitTimeMillisecond      <T extends TimeMillisecond>     (vector: Vector<T>, index: number) { return       getTimeMillisecond(vector, index); }
    public visitTimeMicrosecond      <T extends TimeMicrosecond>     (vector: Vector<T>, index: number) { return       getTimeMicrosecond(vector, index); }
    public visitTimeNanosecond       <T extends TimeNanosecond>      (vector: Vector<T>, index: number) { return        getTimeNanosecond(vector, index); }
    public visitDecimal              <T extends Decimal>             (vector: Vector<T>, index: number) { return               getDecimal(vector, index); }
    public visitList                 <T extends List>                (vector: Vector<T>, index: number) { return                  getList(vector, index); }
    public visitStruct               <T extends Struct>              (vector: Vector<T>, index: number) { return                getNested(vector, index); }
    public visitUnion                <T extends Union>               (vector: Vector<T>, index: number) { return                 getUnion(vector, index); }
    public visitDenseUnion           <T extends DenseUnion>          (vector: Vector<T>, index: number) { return            getDenseUnion(vector, index); }
    public visitSparseUnion          <T extends SparseUnion>         (vector: Vector<T>, index: number) { return           getSparseUnion(vector, index); }
    public visitDictionary           <T extends Dictionary>          (vector: Vector<T>, index: number) { return            getDictionary(vector, index); }
    public visitInterval             <T extends Interval>            (vector: Vector<T>, index: number) { return              getInterval(vector, index); }
    public visitIntervalDayTime      <T extends IntervalDayTime>     (vector: Vector<T>, index: number) { return       getIntervalDayTime(vector, index); }
    public visitIntervalYearMonth    <T extends IntervalYearMonth>   (vector: Vector<T>, index: number) { return     getIntervalYearMonth(vector, index); }
    public visitFixedSizeList        <T extends FixedSizeList>       (vector: Vector<T>, index: number) { return         getFixedSizeList(vector, index); }
    public visitMap                  <T extends Map_>                (vector: Vector<T>, index: number) { return                getNested(vector, index); }
}

export const instance = new GetVisitor();

const getNull = <T extends Null>(_vector: Vector<T>, _index: number): T['TValue'] => null;
const getVariableWidthBytes = (values: Uint8Array, valueOffsets: Int32Array, index: number) => (values.subarray(valueOffsets[index], valueOffsets[index + 1]));
const getBool = <T extends Bool>({ offset, values }: Vector<T>, index: number): T['TValue'] => {
    const idx = offset + index;
    const byte = values[idx >> 3];
    return (byte & 1 << (idx % 8)) !== 0;
}

type Numeric1X = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32 | Float32 | Float64;
type Numeric2X = Int64 | Uint64;

const getDateDay         = <T extends DateDay>        ({ values         }: Vector<T>, index: number): T['TValue'] => (epochDaysToDate(values, index * 2));
const getDateMillisecond = <T extends DateMillisecond>({ values         }: Vector<T>, index: number): T['TValue'] => (epochMillisecondsLongToDate(values, index * 2));
const getNumeric         = <T extends Numeric1X>      ({ stride, values }: Vector<T>, index: number): T['TValue'] => (values[stride * index]);
const getFloat16         = <T extends Float16>        ({ stride, values }: Vector<T>, index: number): T['TValue'] => ((values[stride * index] - 32767) / 32767);
const getNumericX2       = <T extends Numeric2X>      ({ stride, values }: Vector<T>, index: number): T['TValue'] => (values.subarray(stride * index, stride * index + 1));
const getFixedSizeBinary = <T extends FixedSizeBinary>({ stride, values }: Vector<T>, index: number): T['TValue'] => (values.subarray(stride * index, stride * (index + 1)));

const getBinary = <T extends Binary>({ values, valueOffsets }: Vector<T>, index: number): T['TValue'] => getVariableWidthBytes(values, valueOffsets, index);
const getUtf8 = <T extends Utf8>({ values, valueOffsets }: Vector<T>, index: number): T['TValue'] => decodeUtf8(getVariableWidthBytes(values, valueOffsets, index));

const getInt = <T extends Int>(vector: Vector<T>, index: number): T['TValue'] => (
    vector.type.bitWidth < 64
        ? getNumeric(<any> vector, index)
        : getNumericX2(<any> vector, index)
);

const getFloat = <T extends Float> (vector: Vector<T>, index: number): T['TValue'] => (
    vector.type.precision !== Precision.HALF
        ? getNumeric(vector as any, index)
        : getFloat16(vector as any, index)
);

const getDate = <T extends Date_> (vector: Vector<T>, index: number): T['TValue'] => (
    vector.type.unit === DateUnit.DAY
        ? getDateDay(vector as any, index)
        : getDateMillisecond(vector as any, index)
);

const getTimestampSecond      = <T extends TimestampSecond>     ({ values }: Vector<T>, index: number): T['TValue'] => epochSecondsToMs(values, index * 2);
const getTimestampMillisecond = <T extends TimestampMillisecond>({ values }: Vector<T>, index: number): T['TValue'] => epochMillisecondsLongToMs(values, index * 2);
const getTimestampMicrosecond = <T extends TimestampMicrosecond>({ values }: Vector<T>, index: number): T['TValue'] => epochMicrosecondsLongToMs(values, index * 2);
const getTimestampNanosecond  = <T extends TimestampNanosecond> ({ values }: Vector<T>, index: number): T['TValue'] => epochNanosecondsLongToMs(values, index * 2);
const getTimestamp            = <T extends Timestamp>(vector: Vector<T>, index: number): T['TValue'] => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      getTimestampSecond(vector as Vector<TimestampSecond>, index);
        case TimeUnit.MILLISECOND: return getTimestampMillisecond(vector as Vector<TimestampMillisecond>, index);
        case TimeUnit.MICROSECOND: return getTimestampMicrosecond(vector as Vector<TimestampMicrosecond>, index);
        case TimeUnit.NANOSECOND:  return  getTimestampNanosecond(vector as Vector<TimestampNanosecond>, index);
    }
}

const getTimeSecond      = <T extends TimeSecond>     ({ values, stride }: Vector<T>, index: number): T['TValue'] => values[stride * index];
const getTimeMillisecond = <T extends TimeMillisecond>({ values, stride }: Vector<T>, index: number): T['TValue'] => values[stride * index];
const getTimeMicrosecond = <T extends TimeMicrosecond>({ values         }: Vector<T>, index: number): T['TValue'] => values.subarray(2 * index, 2 * index + 1);
const getTimeNanosecond  = <T extends TimeNanosecond> ({ values         }: Vector<T>, index: number): T['TValue'] => values.subarray(2 * index, 2 * index + 1);
const getTime            = <T extends Time>(vector: Vector<T>, index: number): T['TValue'] => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      getTimeSecond(vector as Vector<TimeSecond>, index);
        case TimeUnit.MILLISECOND: return getTimeMillisecond(vector as Vector<TimeMillisecond>, index);
        case TimeUnit.MICROSECOND: return getTimeMicrosecond(vector as Vector<TimeMicrosecond>, index);
        case TimeUnit.NANOSECOND:  return  getTimeNanosecond(vector as Vector<TimeNanosecond>, index);
    }
}

const getDecimal = <T extends Decimal>({ values }: Vector<T>, index: number): T['TValue'] => values.subarray(4 * index, 4 * (index + 1));

const getList = <T extends List>(vector: Vector<T>, index: number): T['TValue'] => {
    const child = vector.getChildAt(0)!, { valueOffsets, stride } = vector;
    return child.slice(valueOffsets[index * stride], valueOffsets[(index * stride) + 1]);
}

const getNested = <
    S extends { [key: string]: DataType },
    V extends Vector<Map_<S>> | Vector<Struct<S>>
>(vector: V, index: number): V['TValue'] => {
    return vector.rowProxy.bind(vector, index);
}

const getUnion = <
    V extends Vector<Union> | Vector<DenseUnion> | Vector<SparseUnion>
>(vector: V, index: number): V['TValue'] => {
    return vector.type.mode === UnionMode.Dense ?
        getDenseUnion(vector as Vector<DenseUnion>, index) :
        getSparseUnion(vector as Vector<SparseUnion>, index);
}

const getDenseUnion = <T extends DenseUnion>(vector: Vector<T>, index: number): T['TValue'] => {
    const { typeIds, type: { typeIdToChildIndex } } = vector;
    const child = vector.getChildAt(typeIdToChildIndex[typeIds[index] as Type]);
    return child ? child.get(vector.valueOffsets[index]) : null;
}

const getSparseUnion = <T extends SparseUnion>(vector: Vector<T>, index: number): T['TValue'] => {
    const { typeIds, type: { typeIdToChildIndex } } = vector;
    const child = vector.getChildAt(typeIdToChildIndex[typeIds[index] as Type]);
    return child ? child.get(index) : null;
}

const getDictionary = <T extends Dictionary>(vector: Vector<T>, index: number): T['TValue'] => {
    const key = vector.indices.get(index) as number;
    const val = vector.type.dictionary.get(key);
    return val;
}

const getInterval = <T extends Interval>(vector: Vector<T>, index: number): T['TValue'] =>
    (vector.type.unit === IntervalUnit.DAY_TIME)
        ? getIntervalDayTime(vector as any, index)
        : getIntervalYearMonth(vector as any, index);

const getIntervalDayTime = <T extends IntervalDayTime>({ values }: Vector<T>, index: number): T['TValue'] => values.subarray(2 * index, 2 * index + 1);

const getIntervalYearMonth = <T extends IntervalYearMonth>({ values }: Vector<T>, index: number): T['TValue'] => {
    const interval = values[2 * index];
    const int32s = new Int32Array(2);
    int32s[0] = interval / 12 | 0; /* years */
    int32s[1] = interval % 12 | 0; /* months */
    return int32s;
}

const getFixedSizeList = <T extends FixedSizeList>(vector: Vector<T>, index: number): T['TValue'] => {
    const child = vector.getChildAt(0)!, { stride } = vector;
    return child.slice(index * stride, (index + 1) * stride);
}
