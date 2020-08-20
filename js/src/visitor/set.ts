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
import { Field } from '../schema';
import { Vector } from '../vector';
import { Visitor } from '../visitor';
import { encodeUtf8 } from '../util/utf8';
import { TypeToDataType } from '../interfaces';
import { float64ToUint16 } from '../util/math';
import { toArrayBufferView } from '../util/buffer';
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

/** @ignore */
export interface SetVisitor extends Visitor {
    visit<T extends Data>(node: T, index: number, value: T['TValue']): void;
    visitMany<T extends Data>(nodes: T[], indices: number[], values: T['TValue'][]): void[];
    getVisitFn<T extends DataType>(node: Data<T> | T): (data: Data<T>, index: number, value: Data<T>['TValue']) => void;
    getVisitFn<T extends Type>(node: T): (data: Data<TypeToDataType<T>>, index: number, value: TypeToDataType<T>['TValue']) => void;
    visitNull                 <T extends Null>                (data: Data<T>, index: number, value: T['TValue']): void;
    visitBool                 <T extends Bool>                (data: Data<T>, index: number, value: T['TValue']): void;
    visitInt                  <T extends Int>                 (data: Data<T>, index: number, value: T['TValue']): void;
    visitInt8                 <T extends Int8>                (data: Data<T>, index: number, value: T['TValue']): void;
    visitInt16                <T extends Int16>               (data: Data<T>, index: number, value: T['TValue']): void;
    visitInt32                <T extends Int32>               (data: Data<T>, index: number, value: T['TValue']): void;
    visitInt64                <T extends Int64>               (data: Data<T>, index: number, value: T['TValue']): void;
    visitUint8                <T extends Uint8>               (data: Data<T>, index: number, value: T['TValue']): void;
    visitUint16               <T extends Uint16>              (data: Data<T>, index: number, value: T['TValue']): void;
    visitUint32               <T extends Uint32>              (data: Data<T>, index: number, value: T['TValue']): void;
    visitUint64               <T extends Uint64>              (data: Data<T>, index: number, value: T['TValue']): void;
    visitFloat                <T extends Float>               (data: Data<T>, index: number, value: T['TValue']): void;
    visitFloat16              <T extends Float16>             (data: Data<T>, index: number, value: T['TValue']): void;
    visitFloat32              <T extends Float32>             (data: Data<T>, index: number, value: T['TValue']): void;
    visitFloat64              <T extends Float64>             (data: Data<T>, index: number, value: T['TValue']): void;
    visitUtf8                 <T extends Utf8>                (data: Data<T>, index: number, value: T['TValue']): void;
    visitBinary               <T extends Binary>              (data: Data<T>, index: number, value: T['TValue']): void;
    visitFixedSizeBinary      <T extends FixedSizeBinary>     (data: Data<T>, index: number, value: T['TValue']): void;
    visitDate                 <T extends Date_>               (data: Data<T>, index: number, value: T['TValue']): void;
    visitDateDay              <T extends DateDay>             (data: Data<T>, index: number, value: T['TValue']): void;
    visitDateMillisecond      <T extends DateMillisecond>     (data: Data<T>, index: number, value: T['TValue']): void;
    visitTimestamp            <T extends Timestamp>           (data: Data<T>, index: number, value: T['TValue']): void;
    visitTimestampSecond      <T extends TimestampSecond>     (data: Data<T>, index: number, value: T['TValue']): void;
    visitTimestampMillisecond <T extends TimestampMillisecond>(data: Data<T>, index: number, value: T['TValue']): void;
    visitTimestampMicrosecond <T extends TimestampMicrosecond>(data: Data<T>, index: number, value: T['TValue']): void;
    visitTimestampNanosecond  <T extends TimestampNanosecond> (data: Data<T>, index: number, value: T['TValue']): void;
    visitTime                 <T extends Time>                (data: Data<T>, index: number, value: T['TValue']): void;
    visitTimeSecond           <T extends TimeSecond>          (data: Data<T>, index: number, value: T['TValue']): void;
    visitTimeMillisecond      <T extends TimeMillisecond>     (data: Data<T>, index: number, value: T['TValue']): void;
    visitTimeMicrosecond      <T extends TimeMicrosecond>     (data: Data<T>, index: number, value: T['TValue']): void;
    visitTimeNanosecond       <T extends TimeNanosecond>      (data: Data<T>, index: number, value: T['TValue']): void;
    visitDecimal              <T extends Decimal>             (data: Data<T>, index: number, value: T['TValue']): void;
    visitList                 <T extends List>                (data: Data<T>, index: number, value: T['TValue']): void;
    visitStruct               <T extends Struct>              (data: Data<T>, index: number, value: T['TValue']): void;
    visitUnion                <T extends Union>               (data: Data<T>, index: number, value: T['TValue']): void;
    visitDenseUnion           <T extends DenseUnion>          (data: Data<T>, index: number, value: T['TValue']): void;
    visitSparseUnion          <T extends SparseUnion>         (data: Data<T>, index: number, value: T['TValue']): void;
    visitDictionary           <T extends Dictionary>          (data: Data<T>, index: number, value: T['TValue']): void;
    visitInterval             <T extends Interval>            (data: Data<T>, index: number, value: T['TValue']): void;
    visitIntervalDayTime      <T extends IntervalDayTime>     (data: Data<T>, index: number, value: T['TValue']): void;
    visitIntervalYearMonth    <T extends IntervalYearMonth>   (data: Data<T>, index: number, value: T['TValue']): void;
    visitFixedSizeList        <T extends FixedSizeList>       (data: Data<T>, index: number, value: T['TValue']): void;
    visitMap                  <T extends Map_>                (data: Data<T>, index: number, value: T['TValue']): void;
}

/** @ignore */
export class SetVisitor extends Visitor {}

/** @ignore */
const setEpochMsToDays = (data: Int32Array, index: number, epochMs: number) => { data[index] = (epochMs / 86400000) | 0; };
/** @ignore */
const setEpochMsToMillisecondsLong = (data: Int32Array, index: number, epochMs: number) => {
    data[index] = (epochMs % 4294967296) | 0;
    data[index + 1] = (epochMs / 4294967296) | 0;
};
/** @ignore */
const setEpochMsToMicrosecondsLong = (data: Int32Array, index: number, epochMs: number) => {
    data[index] = ((epochMs * 1000) % 4294967296) | 0;
    data[index + 1] = ((epochMs * 1000) / 4294967296) | 0;
};
/** @ignore */
const setEpochMsToNanosecondsLong = (data: Int32Array, index: number, epochMs: number) => {
    data[index] = ((epochMs * 1000000) % 4294967296) | 0;
    data[index + 1] = ((epochMs * 1000000) / 4294967296) | 0;
};

/** @ignore */
const setVariableWidthBytes = (values: Uint8Array, valueOffsets: Int32Array, index: number, value: Uint8Array) => {
    const { [index]: x, [index + 1]: y } = valueOffsets;
    if (x != null && y != null) {
        values.set(value.subarray(0, y - x), x);
    }
};

/** @ignore */
const setBool = <T extends Bool>({ offset, values }: Data<T>, index: number, val: boolean) => {
    const idx = offset + index;
    val ? (values[idx >> 3] |=  (1 << (idx % 8)))  // true
        : (values[idx >> 3] &= ~(1 << (idx % 8))); // false

};

/** @ignore */ type Numeric1X = Int8 | Int16 | Int32 | Uint8 | Uint16 | Uint32 | Float32 | Float64;
/** @ignore */ type Numeric2X = Int64 | Uint64;

/** @ignore */
const setDateDay         = <T extends DateDay>        ({ values         }: Data<T>, index: number, value: T['TValue']): void => { setEpochMsToDays(values, index, value.valueOf()); };
/** @ignore */
const setDateMillisecond = <T extends DateMillisecond>({ values         }: Data<T>, index: number, value: T['TValue']): void => { setEpochMsToMillisecondsLong(values, index * 2, value.valueOf()); };
/** @ignore */
const setNumeric         = <T extends Numeric1X>      ({ stride, values }: Data<T>, index: number, value: T['TValue']): void => { values[stride * index] = value; };
/** @ignore */
const setFloat16         = <T extends Float16>        ({ stride, values }: Data<T>, index: number, value: T['TValue']): void => { values[stride * index] = float64ToUint16(value); };
/** @ignore */
const setNumericX2       = <T extends Numeric2X>      (data: Data<T>, index: number, value: T['TValue']): void => {
    switch (typeof value) {
        case 'bigint': data.values64![index] = value; break;
        case 'number': data.values[index * data.stride] = value; break;
        default:
            const val = value as T['TArray'];
            const { stride, ArrayType } = data;
            const long = toArrayBufferView<T['TArray']>(ArrayType, val);
            data.values.set(long.subarray(0, stride), stride * index);
    }
};
/** @ignore */
const setFixedSizeBinary = <T extends FixedSizeBinary>({ stride, values }: Data<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, stride), stride * index); };

/** @ignore */
const setBinary = <T extends Binary>({ values, valueOffsets }: Data<T>, index: number, value: T['TValue']) => setVariableWidthBytes(values, valueOffsets, index, value);
/** @ignore */
const setUtf8 = <T extends Utf8>({ values, valueOffsets }: Data<T>, index: number, value: T['TValue']) => {
    setVariableWidthBytes(values, valueOffsets, index, encodeUtf8(value));
};

/* istanbul ignore next */
/** @ignore */
const setInt = <T extends Int>(data: Data<T>, index: number, value: T['TValue']): void => {
    data.type.bitWidth < 64
        ? setNumeric(data as Data<Numeric1X>, index, value as Numeric1X['TValue'])
        : setNumericX2(data as Data<Numeric2X>, index, value as Numeric2X['TValue']);
};

/* istanbul ignore next */
/** @ignore */
const setFloat = <T extends Float>(data: Data<T>, index: number, value: T['TValue']): void => {
    data.type.precision !== Precision.HALF
        ? setNumeric(data as Data<Numeric1X>, index, value)
        : setFloat16(data as Data<Float16>, index, value);
};

/* istanbul ignore next */
const setDate = <T extends Date_> (data: Data<T>, index: number, value: T['TValue']): void => {
    data.type.unit === DateUnit.DAY
        ? setDateDay(data as Data<DateDay>, index, value)
        : setDateMillisecond(data as Data<DateMillisecond>, index, value);
};

/** @ignore */
const setTimestampSecond      = <T extends TimestampSecond>     ({ values }: Data<T>, index: number, value: T['TValue']): void => setEpochMsToMillisecondsLong(values, index * 2, value / 1000);
/** @ignore */
const setTimestampMillisecond = <T extends TimestampMillisecond>({ values }: Data<T>, index: number, value: T['TValue']): void => setEpochMsToMillisecondsLong(values, index * 2, value);
/** @ignore */
const setTimestampMicrosecond = <T extends TimestampMicrosecond>({ values }: Data<T>, index: number, value: T['TValue']): void => setEpochMsToMicrosecondsLong(values, index * 2, value);
/** @ignore */
const setTimestampNanosecond  = <T extends TimestampNanosecond> ({ values }: Data<T>, index: number, value: T['TValue']): void => setEpochMsToNanosecondsLong(values, index * 2, value);
/* istanbul ignore next */
/** @ignore */
const setTimestamp            = <T extends Timestamp>(data: Data<T>, index: number, value: T['TValue']): void => {
    switch (data.type.unit) {
        case TimeUnit.SECOND:      return      setTimestampSecond(data as Data<TimestampSecond>, index, value);
        case TimeUnit.MILLISECOND: return setTimestampMillisecond(data as Data<TimestampMillisecond>, index, value);
        case TimeUnit.MICROSECOND: return setTimestampMicrosecond(data as Data<TimestampMicrosecond>, index, value);
        case TimeUnit.NANOSECOND:  return  setTimestampNanosecond(data as Data<TimestampNanosecond>, index, value);
    }
};

/** @ignore */
const setTimeSecond      = <T extends TimeSecond>     ({ values, stride }: Data<T>, index: number, value: T['TValue']): void => { values[stride * index] = value; };
/** @ignore */
const setTimeMillisecond = <T extends TimeMillisecond>({ values, stride }: Data<T>, index: number, value: T['TValue']): void => { values[stride * index] = value; };
/** @ignore */
const setTimeMicrosecond = <T extends TimeMicrosecond>({ values         }: Data<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 2), 2 * index); };
/** @ignore */
const setTimeNanosecond  = <T extends TimeNanosecond> ({ values         }: Data<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 2), 2 * index); };
/* istanbul ignore next */
/** @ignore */
const setTime            = <T extends Time>(data: Data<T>, index: number, value: T['TValue']): void => {
    switch (data.type.unit) {
        case TimeUnit.SECOND:      return      setTimeSecond(data as Data<TimeSecond>, index, value as TimeSecond['TValue']);
        case TimeUnit.MILLISECOND: return setTimeMillisecond(data as Data<TimeMillisecond>, index, value as TimeMillisecond['TValue']);
        case TimeUnit.MICROSECOND: return setTimeMicrosecond(data as Data<TimeMicrosecond>, index, value as TimeMicrosecond['TValue']);
        case TimeUnit.NANOSECOND:  return  setTimeNanosecond(data as Data<TimeNanosecond>, index, value as TimeNanosecond['TValue']);
    }
};

/** @ignore */
const setDecimal = <T extends Decimal>({ values }: Data<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 4), 4 * index); };

/** @ignore */
const setList = <T extends List>(data: Data<T>, index: number, value: T['TValue']): void => {
    const values = data.childData[0];
    const valueOffsets = data.valueOffsets;
    const set = instance.getVisitFn(values);
    for (let idx = -1, itr = valueOffsets[index], end = valueOffsets[index + 1]; itr < end;) {
        set(values, itr++, value.get(++idx));
    }
};

/** @ignore */
const setMap = <T extends Map_>(data: Data<T>, index: number, value: T['TValue']) => {
    const values = data.childData[0];
    const valueOffsets = data.valueOffsets;
    const set = instance.getVisitFn(values);
    let { [index]: idx, [index + 1]: end } = valueOffsets;
    const entries = value instanceof Map ? value.entries() : Object.entries(value);
    for (const val of entries) {
        set(values, idx, val);
        if (++idx >= end) break;
    }
};

/** @ignore */ type SetFunc<T extends DataType> = (data: Data<T>, i: number, v: T['TValue']) => void;

/** @ignore */ const _setStructArrayValue = (o: number, v: any[]) =>
    <T extends DataType>(set: SetFunc<T>, c: Data<T>, _: Field, i: number) => c && set(c, o, v[i]);

/** @ignore */ const _setStructVectorValue = (o: number, v: Vector) =>
    <T extends DataType>(set: SetFunc<T>, c: Data<T>, _: Field, i: number) => c && set(c, o, v.get(i));

/** @ignore */ const _setStructMapValue = (o: number, v: Map<string, any>) =>
    <T extends DataType>(set: SetFunc<T>, c: Data<T>, f: Field, _: number) => c && set(c, o, v.get(f.name));

/** @ignore */ const _setStructObjectValue = (o: number, v: { [key: string]: any }) =>
    <T extends DataType>(set: SetFunc<T>, c: Data<T>, f: Field, _: number) => c && set(c, o, v[f.name]);

/** @ignore */
const setStruct = <T extends Struct>(data: Data<T>, index: number, value: T['TValue']) => {

    const childSetters = data.type.children.map((f) => instance.getVisitFn(f.type));
    const set = value instanceof Map    ? _setStructMapValue(index, value)    :
                value instanceof Vector ? _setStructVectorValue(index, value) :
                Array.isArray(value)    ? _setStructArrayValue(index, value)  :
                                          _setStructObjectValue(index, value) ;

    data.type.children.forEach((f: Field, i: number) => set(childSetters[i], data.childData[i], f, i));
};

/* istanbul ignore next */
/** @ignore */
const setUnion = <
    V extends Data<Union> | Data<DenseUnion> | Data<SparseUnion>
>(data: V, index: number, value: V['TValue']) => {
    data.type.mode === UnionMode.Dense ?
        setDenseUnion(data as Data<DenseUnion>, index, value) :
        setSparseUnion(data as Data<SparseUnion>, index, value);
};

/** @ignore */
const setDenseUnion = <T extends DenseUnion>(data: Data<T>, index: number, value: T['TValue']): void => {
    const childIndex = data.type.typeIdToChildIndex[data.typeIds[index]];
    const child = data.childData[childIndex];
    instance.visit(child, data.valueOffsets[index], value);
};

/** @ignore */
const setSparseUnion = <T extends SparseUnion>(data: Data<T>, index: number, value: T['TValue']): void => {
    const childIndex = data.type.typeIdToChildIndex[data.typeIds[index]];
    const child = data.childData[childIndex];
    instance.visit(child, index, value);
};

/** @ignore */
const setDictionary = <T extends Dictionary>(data: Data<T>, index: number, value: T['TValue']): void => {
    data.dictionary?.set(data.values[index], value);
};

/* istanbul ignore next */
/** @ignore */
const setIntervalValue = <T extends Interval>(data: Data<T>, index: number, value: T['TValue']): void => {
    (data.type.unit === IntervalUnit.DAY_TIME)
        ? setIntervalDayTime(data as Data<IntervalDayTime>, index, value)
        : setIntervalYearMonth(data as Data<IntervalYearMonth>, index, value);
};

/** @ignore */
const setIntervalDayTime = <T extends IntervalDayTime>({ values }: Data<T>, index: number, value: T['TValue']): void => { values.set(value.subarray(0, 2), 2 * index); };
/** @ignore */
const setIntervalYearMonth = <T extends IntervalYearMonth>({ values }: Data<T>, index: number, value: T['TValue']): void => { values[index] = (value[0] * 12) + (value[1] % 12); };

/** @ignore */
const setFixedSizeList = <T extends FixedSizeList>(data: Data<T>, index: number, value: T['TValue']): void => {
    const { stride } = data;
    const child = data.childData[0];
    const set = instance.getVisitFn(child);
    for (let idx = -1, offset = index * stride; ++idx < stride;) {
        set(child, offset + idx, value.get(idx));
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
SetVisitor.prototype.visitDate                 =                 setDate;
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
SetVisitor.prototype.visitStruct               =               setStruct;
SetVisitor.prototype.visitUnion                =                setUnion;
SetVisitor.prototype.visitDenseUnion           =           setDenseUnion;
SetVisitor.prototype.visitSparseUnion          =          setSparseUnion;
SetVisitor.prototype.visitDictionary           =           setDictionary;
SetVisitor.prototype.visitInterval             =        setIntervalValue;
SetVisitor.prototype.visitIntervalDayTime      =      setIntervalDayTime;
SetVisitor.prototype.visitIntervalYearMonth    =    setIntervalYearMonth;
SetVisitor.prototype.visitFixedSizeList        =        setFixedSizeList;
SetVisitor.prototype.visitMap                  =                  setMap;

/** @ignore */
export const instance = new SetVisitor();
