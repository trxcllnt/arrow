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
import { TextEncoder } from 'text-encoding-utf-8';
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

export const encodeUtf8 = ((encoder) =>
    encoder.encode.bind(encoder) as (input?: string, options?: { stream?: boolean }) => Uint8Array
)(new TextEncoder('utf-8'));

export interface SetVisitor extends Visitor {
    visitMany <T extends Vector>  (nodes: T[], indices: number[], values: T['TValue'][]): void[];
    visit     <T extends Vector>  (node: T, index: number, value: T['TValue']          ): void;
    getVisitFn<T extends Type>    (node: T        ): (vector: Vector<T>, index: number, value: Vector<T>['TValue']) => void;
    getVisitFn<T extends DataType>(node: Vector<T>): (vector: Vector<T>, index: number, value: Vector<T>['TValue']) => void;
    getVisitFn<T extends DataType>(node: Data<T>  ): (vector: Vector<T>, index: number, value: Vector<T>['TValue']) => void;
    getVisitFn<T extends DataType>(node: T        ): (vector: Vector<T>, index: number, value: Vector<T>['TValue']) => void;
}

export class SetVisitor extends Visitor {
    public visitNull                 <T extends Null>                (_: Vector<T>) {                                                                                                }
    public visitBool                 <T extends Bool>                (vector: Vector<T>, index: number, value: T['TValue']) { return                  setBool(vector, index, value); }
    public visitInt                  <T extends Int>                 (vector: Vector<T>, index: number, value: T['TValue']) { return                   setInt(vector, index, value); }
    public visitInt8                 <T extends Int8>                (vector: Vector<T>, index: number, value: T['TValue']) { return               setNumeric(vector, index, value); }
    public visitInt16                <T extends Int16>               (vector: Vector<T>, index: number, value: T['TValue']) { return               setNumeric(vector, index, value); }
    public visitInt32                <T extends Int32>               (vector: Vector<T>, index: number, value: T['TValue']) { return               setNumeric(vector, index, value); }
    public visitInt64                <T extends Int64>               (vector: Vector<T>, index: number, value: T['TValue']) { return             setNumericX2(vector, index, value); }
    public visitUint8                <T extends Uint8>               (vector: Vector<T>, index: number, value: T['TValue']) { return               setNumeric(vector, index, value); }
    public visitUint16               <T extends Uint16>              (vector: Vector<T>, index: number, value: T['TValue']) { return               setNumeric(vector, index, value); }
    public visitUint32               <T extends Uint32>              (vector: Vector<T>, index: number, value: T['TValue']) { return               setNumeric(vector, index, value); }
    public visitUint64               <T extends Uint64>              (vector: Vector<T>, index: number, value: T['TValue']) { return             setNumericX2(vector, index, value); }
    public visitFloat                <T extends Float>               (vector: Vector<T>, index: number, value: T['TValue']) { return                 setFloat(vector, index, value); }
    public visitFloat16              <T extends Float16>             (vector: Vector<T>, index: number, value: T['TValue']) { return               setFloat16(vector, index, value); }
    public visitFloat32              <T extends Float32>             (vector: Vector<T>, index: number, value: T['TValue']) { return               setNumeric(vector, index, value); }
    public visitFloat64              <T extends Float64>             (vector: Vector<T>, index: number, value: T['TValue']) { return               setNumeric(vector, index, value); }
    public visitUtf8                 <T extends Utf8>                (vector: Vector<T>, index: number, value: T['TValue']) { return                  setUtf8(vector, index, value); }
    public visitBinary               <T extends Binary>              (vector: Vector<T>, index: number, value: T['TValue']) { return                setBinary(vector, index, value); }
    public visitFixedSizeBinary      <T extends FixedSizeBinary>     (vector: Vector<T>, index: number, value: T['TValue']) { return       setFixedSizeBinary(vector, index, value); }
    public visitDate                 <T extends Date_>               (vector: Vector<T>, index: number, value: T['TValue']) { return                  getDate(vector, index, value); }
    public visitDateDay              <T extends DateDay>             (vector: Vector<T>, index: number, value: T['TValue']) { return               setDateDay(vector, index, value); }
    public visitDateMillisecond      <T extends DateMillisecond>     (vector: Vector<T>, index: number, value: T['TValue']) { return       setDateMillisecond(vector, index, value); }
    public visitTimestamp            <T extends Timestamp>           (vector: Vector<T>, index: number, value: T['TValue']) { return             setTimestamp(vector, index, value); }
    public visitTimestampSecond      <T extends TimestampSecond>     (vector: Vector<T>, index: number, value: T['TValue']) { return       setTimestampSecond(vector, index, value); }
    public visitTimestampMillisecond <T extends TimestampMillisecond>(vector: Vector<T>, index: number, value: T['TValue']) { return  setTimestampMillisecond(vector, index, value); }
    public visitTimestampMicrosecond <T extends TimestampMicrosecond>(vector: Vector<T>, index: number, value: T['TValue']) { return  setTimestampMicrosecond(vector, index, value); }
    public visitTimestampNanosecond  <T extends TimestampNanosecond> (vector: Vector<T>, index: number, value: T['TValue']) { return   setTimestampNanosecond(vector, index, value); }
    public visitTime                 <T extends Time>                (vector: Vector<T>, index: number, value: T['TValue']) { return                  setTime(vector, index, value); }
    public visitTimeSecond           <T extends TimeSecond>          (vector: Vector<T>, index: number, value: T['TValue']) { return            setTimeSecond(vector, index, value); }
    public visitTimeMillisecond      <T extends TimeMillisecond>     (vector: Vector<T>, index: number, value: T['TValue']) { return       setTimeMillisecond(vector, index, value); }
    public visitTimeMicrosecond      <T extends TimeMicrosecond>     (vector: Vector<T>, index: number, value: T['TValue']) { return       setTimeMicrosecond(vector, index, value); }
    public visitTimeNanosecond       <T extends TimeNanosecond>      (vector: Vector<T>, index: number, value: T['TValue']) { return        setTimeNanosecond(vector, index, value); }
    public visitDecimal              <T extends Decimal>             (vector: Vector<T>, index: number, value: T['TValue']) { return               setDecimal(vector, index, value); }
    public visitList                 <T extends List>                (vector: Vector<T>, index: number, value: T['TValue']) { return                  setList(vector, index, value); }
    public visitStruct               <T extends Struct>              (vector: Vector<T>, index: number, value: T['TValue']) { return                setNested(vector, index, value); }
    public visitUnion                <T extends Union>               (vector: Vector<T>, index: number, value: T['TValue']) { return                 setUnion(vector, index, value); }
    public visitDenseUnion           <T extends DenseUnion>          (vector: Vector<T>, index: number, value: T['TValue']) { return            setDenseUnion(vector, index, value); }
    public visitSparseUnion          <T extends SparseUnion>         (vector: Vector<T>, index: number, value: T['TValue']) { return           setSparseUnion(vector, index, value); }
    public visitDictionary           <T extends Dictionary>          (vector: Vector<T>, index: number, value: T['TValue']) { return            setDictionary(vector, index, value); }
    public visitInterval             <T extends Interval>            (vector: Vector<T>, index: number, value: T['TValue']) { return         setIntervalValue(vector, index, value); }
    public visitIntervalDayTime      <T extends IntervalDayTime>     (vector: Vector<T>, index: number, value: T['TValue']) { return       setIntervalDayTime(vector, index, value); }
    public visitIntervalYearMonth    <T extends IntervalYearMonth>   (vector: Vector<T>, index: number, value: T['TValue']) { return     setIntervalYearMonth(vector, index, value); }
    public visitFixedSizeList        <T extends FixedSizeList>       (vector: Vector<T>, index: number, value: T['TValue']) { return         setFixedSizeList(vector, index, value); }
    public visitMap                  <T extends Map_>                (vector: Vector<T>, index: number, value: T['TValue']) { return                setNested(vector, index, value); }
}

export const instance = new SetVisitor();

const setEpochMsToSeconds = (data: Int32Array, index: number, epochMs: number) => { data[index] = (epochMs / 1000) | 0; }
const setEpochMsToDays = (data: Int32Array, index: number, epochMs: number) => { data[index] = (epochMs / 86400000) | 0; };
const setEpochMsToMillisecondsLong = (data: Int32Array, index: number, epochMs: number) => {
    data[index] = (epochMs % 4294967296) | 0;
    data[index + 1] = (epochMs / 4294967296) | 0;
};
const setEpochMsToMicrosecondsLong = (data: Int32Array, index: number, epochMs: number) => {
    data[index] = ((epochMs / 1000) % 4294967296) | 0;
    data[index + 1] = ((epochMs / 1000) / 4294967296) | 0;
};
const setEpochMsToNanosecondsLong = (data: Int32Array, index: number, epochMs: number) => {
    data[index] = ((epochMs / 1000000) % 4294967296) | 0;
    data[index + 1] = ((epochMs / 1000000) / 4294967296) | 0;
};

const setVariableWidthBytes = (values: Uint8Array, valueOffsets: Int32Array, index: number, value: Uint8Array) => {
    const { [index]: x, [index + 1]: y } = valueOffsets;
    if (x != null && y != null) {
        values.set(value.subarray(0, y - x), x);
    }
};

const setBool = <T extends Bool>({ offset, values }: Vector<T>, index: number, val: boolean) => {
    const idx = offset + index;
    val ? (values[idx >> 3] |=  (1 << (idx % 8)))  // true
        : (values[idx >> 3] &= ~(1 << (idx % 8))); // false

};

type Numeric1X = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32 | Float32 | Float64;
type Numeric2X = Int64 | Uint64;

const setDateDay         = <T extends DateDay>        ({ values         }: Vector<T>, index: number, value: T['TValue']): void => { setEpochMsToDays(values, index, value.valueOf()); };
const setDateMillisecond = <T extends DateMillisecond>({ values         }: Vector<T>, index: number, value: T['TValue']): void => { setEpochMsToMillisecondsLong(values, index * 2, value.valueOf()); };
const setNumeric         = <T extends Numeric1X>      ({ stride, values }: Vector<T>, index: number, value: T['TValue']): void => { values[stride * index] = value; };
const setFloat16         = <T extends Float16>        ({ stride, values }: Vector<T>, index: number, value: T['TValue']): void => { values[stride * index] = (value * 32767) + 32767; };
const setNumericX2       = <T extends Numeric2X>      ({ stride, values }: Vector<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, stride), stride * index); };
const setFixedSizeBinary = <T extends FixedSizeBinary>({ stride, values }: Vector<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, stride), stride * index); };

const setBinary = <T extends Binary>({ values, valueOffsets }: Vector<T>, index: number, value: T['TValue']) => setVariableWidthBytes(values, valueOffsets, index, value);
const setUtf8 = <T extends Utf8>({ values, valueOffsets }: Vector<T>, index: number, value: T['TValue']) => {
    setVariableWidthBytes(values, valueOffsets, index, encodeUtf8(value));
};

const setInt = <T extends Int>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    vector.type.bitWidth < 64
        ? setNumeric(<any> vector, index, value as Numeric1X['TValue'])
        : setNumericX2(<any> vector, index, value as Numeric2X['TValue'])
};

const setFloat = <T extends Float>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    vector.type.precision !== Precision.HALF
        ? setNumeric(<any> vector, index, value)
        : setFloat16(<any> vector, index, value)
};

const getDate = <T extends Date_> (vector: Vector<T>, index: number, value: T['TValue']): void => {
    vector.type.unit === DateUnit.DAY
        ? setDateDay(vector, index, value)
        : setDateMillisecond(vector, index, value)
};

const setTimestampSecond      = <T extends TimestampSecond>     ({ values }: Vector<T>, index: number, value: T['TValue']): void => setEpochMsToSeconds(values, index * 2, value);
const setTimestampMillisecond = <T extends TimestampMillisecond>({ values }: Vector<T>, index: number, value: T['TValue']): void => setEpochMsToMillisecondsLong(values, index * 2, value);
const setTimestampMicrosecond = <T extends TimestampMicrosecond>({ values }: Vector<T>, index: number, value: T['TValue']): void => setEpochMsToMicrosecondsLong(values, index * 2, value);
const setTimestampNanosecond  = <T extends TimestampNanosecond> ({ values }: Vector<T>, index: number, value: T['TValue']): void => setEpochMsToNanosecondsLong(values, index * 2, value);
const setTimestamp            = <T extends Timestamp>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      setTimestampSecond(vector as Vector<TimestampSecond>, index, value);
        case TimeUnit.MILLISECOND: return setTimestampMillisecond(vector as Vector<TimestampMillisecond>, index, value);
        case TimeUnit.MICROSECOND: return setTimestampMicrosecond(vector as Vector<TimestampMicrosecond>, index, value);
        case TimeUnit.NANOSECOND:  return  setTimestampNanosecond(vector as Vector<TimestampNanosecond>, index, value);
    }
};

const setTimeSecond      = <T extends TimeSecond>     ({ values, stride }: Vector<T>, index: number, value: T['TValue']): void => { values[stride * index] = value; };
const setTimeMillisecond = <T extends TimeMillisecond>({ values, stride }: Vector<T>, index: number, value: T['TValue']): void => { values[stride * index] = value; };
const setTimeMicrosecond = <T extends TimeMicrosecond>({ values         }: Vector<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 2), 2 * index); };
const setTimeNanosecond  = <T extends TimeNanosecond> ({ values         }: Vector<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 2), 2 * index); };
const setTime            = <T extends Time>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    switch (vector.type.unit) {
        case TimeUnit.SECOND:      return      setTimeSecond(vector as Vector<TimeSecond>, index, value as TimeSecond['TValue']);
        case TimeUnit.MILLISECOND: return setTimeMillisecond(vector as Vector<TimeMillisecond>, index, value as TimeMillisecond['TValue']);
        case TimeUnit.MICROSECOND: return setTimeMicrosecond(vector as Vector<TimeMicrosecond>, index, value as TimeMicrosecond['TValue']);
        case TimeUnit.NANOSECOND:  return  setTimeNanosecond(vector as Vector<TimeNanosecond>, index, value as TimeNanosecond['TValue']);
    }
};

const setDecimal = <T extends Decimal>({ values }: Vector<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 4), 4 * index); };

const setList = <T extends List>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    const values = vector.getChildAt(0)!;
    const { valueOffsets, stride } = vector;
    let idx = -1, offset = valueOffsets[index * stride];
    let end = Math.min(value.length, valueOffsets[(index * stride) + 1] - offset);
    while (offset < end) {
        values.set(offset++, value.get(++idx));
    }
};

const setNested = <
    S extends { [key: string]: DataType },
    V extends Vector<Map_<S>> | Vector<Struct<S>>
>(vector: V, index: number, value: V['TValue']) => {
    vector.type.children.forEach(({ name }, idx) => {
        const kid = vector.getChildAt(idx);
        kid && kid.set(index, value[name]);
    });
};

const setUnion = <
    V extends Vector<Union> | Vector<DenseUnion> | Vector<SparseUnion>
>(vector: V, index: number, value: V['TValue']) => {
    vector.type.mode === UnionMode.Dense ?
        setDenseUnion(vector as Vector<DenseUnion>, index, value) :
        setSparseUnion(vector as Vector<SparseUnion>, index, value);
};

const setDenseUnion = <T extends DenseUnion>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    const { typeIds, type: { typeIdToChildIndex } } = vector;
    const child = vector.getChildAt(typeIdToChildIndex[typeIds[index] as Type]);
    child && child.set(vector.valueOffsets[index], value);
};

const setSparseUnion = <T extends SparseUnion>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    const { typeIds, type: { typeIdToChildIndex } } = vector;
    const child = vector.getChildAt(typeIdToChildIndex[typeIds[index] as Type]);
    child && child.set(index, value);
};

const setDictionary = <T extends Dictionary>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    return vector.dictionary.set(vector.indices.get(index) as number, value);
};

const setIntervalValue = <T extends Interval>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    (vector.type.unit === IntervalUnit.DAY_TIME)
        ? setIntervalDayTime(vector, index, value)
        : setIntervalYearMonth(vector, index, value);
};

const setIntervalDayTime = <T extends IntervalDayTime>({ values }: Vector<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 2), 2 * index); }
const setIntervalYearMonth = <T extends IntervalYearMonth>({ values }: Vector<T>, index: number, value: T['TValue']): void => { values[index] = (value[0] * 12) + (value[1] % 12); };

const setFixedSizeList = <T extends FixedSizeList>(vector: Vector<T>, index: number, value: T['TValue']): void => {
    const child = vector.getChildAt(0)!, { stride } = vector;
    for (let idx = -1, offset = index * stride; ++idx < stride;) {
        child.set(offset + idx, value.get(++idx));
    }
};

SetVisitor.prototype.visitBool                 =                 setBool;
SetVisitor.prototype.visitInt                  =                  setInt;
SetVisitor.prototype.visitInt8                 =              setNumeric;
SetVisitor.prototype.visitInt16                =              setNumeric;
SetVisitor.prototype.visitInt32                =              setNumeric;
SetVisitor.prototype.visitInt64                =            setNumericX2;
SetVisitor.prototype.visitUint8                =              setNumeric;
SetVisitor.prototype.visitUint16               =              setNumeric;
SetVisitor.prototype.visitUint32               =              setNumeric;
SetVisitor.prototype.visitUint64               =            setNumericX2;
SetVisitor.prototype.visitFloat                =                setFloat;
SetVisitor.prototype.visitFloat16              =              setFloat16;
SetVisitor.prototype.visitFloat32              =              setNumeric;
SetVisitor.prototype.visitFloat64              =              setNumeric;
SetVisitor.prototype.visitUtf8                 =                 setUtf8;
SetVisitor.prototype.visitBinary               =               setBinary;
SetVisitor.prototype.visitFixedSizeBinary      =      setFixedSizeBinary;
SetVisitor.prototype.visitDate                 =                 getDate;
SetVisitor.prototype.visitDateDay              =              setDateDay;
SetVisitor.prototype.visitDateMillisecond      =      setDateMillisecond;
SetVisitor.prototype.visitTimestamp            =            setTimestamp;
SetVisitor.prototype.visitTimestampSecond      =      setTimestampSecond;
SetVisitor.prototype.visitTimestampMillisecond = setTimestampMillisecond;
SetVisitor.prototype.visitTimestampMicrosecond = setTimestampMicrosecond;
SetVisitor.prototype.visitTimestampNanosecond  =  setTimestampNanosecond;
SetVisitor.prototype.visitTime                 =                 setTime;
SetVisitor.prototype.visitTimeSecond           =           setTimeSecond;
SetVisitor.prototype.visitTimeMillisecond      =      setTimeMillisecond;
SetVisitor.prototype.visitTimeMicrosecond      =      setTimeMicrosecond;
SetVisitor.prototype.visitTimeNanosecond       =       setTimeNanosecond;
SetVisitor.prototype.visitDecimal              =              setDecimal;
SetVisitor.prototype.visitList                 =                 setList;
SetVisitor.prototype.visitStruct               =               setNested;
SetVisitor.prototype.visitUnion                =                setUnion;
SetVisitor.prototype.visitDenseUnion           =           setDenseUnion;
SetVisitor.prototype.visitSparseUnion          =          setSparseUnion;
SetVisitor.prototype.visitDictionary           =           setDictionary;
SetVisitor.prototype.visitInterval             =        setIntervalValue;
SetVisitor.prototype.visitIntervalDayTime      =      setIntervalDayTime;
SetVisitor.prototype.visitIntervalYearMonth    =    setIntervalYearMonth;
SetVisitor.prototype.visitFixedSizeList        =        setFixedSizeList;
SetVisitor.prototype.visitMap                  =               setNested;
