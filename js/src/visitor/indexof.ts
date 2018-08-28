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
import { Type } from '../enum';
import { Visitor } from '../visitor';
import { Vector } from '../interfaces';
import { Row } from '../type';
import { getBool, iterateBits } from '../util/bit';
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

export interface IndexOfVisitor extends Visitor {
    visitMany <T extends Vector>  (nodes: T[])                                             : number[];
    visit     <T extends Vector>  (node: T, ...args: any[])                                : number;
    getVisitFn<T extends Type>    (node: T)         : (vector: Vector<T>, ...args: any[]) => number;
    getVisitFn<T extends DataType>(node: Vector<T>) : (vector: Vector<T>, ...args: any[]) => number;
    getVisitFn<T extends DataType>(node: Data<T>)   : (vector: Vector<T>, ...args: any[]) => number;
    getVisitFn<T extends DataType>(node: T)         : (vector: Vector<T>, ...args: any[]) => number;
}

export class IndexOfVisitor extends Visitor {
    public visitNull                 <T extends Null>                (vector: Vector<T>, value: T['TValue'] | null, index: number) { return       nullIndexOf(vector, index, value); }
    public visitBool                 <T extends Bool>                (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitInt                  <T extends Int>                 (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitInt8                 <T extends Int8>                (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitInt16                <T extends Int16>               (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitInt32                <T extends Int32>               (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitInt64                <T extends Int64>               (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitUint8                <T extends Uint8>               (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitUint16               <T extends Uint16>              (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitUint32               <T extends Uint32>              (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitUint64               <T extends Uint64>              (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitFloat                <T extends Float>               (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitFloat16              <T extends Float16>             (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitFloat32              <T extends Float32>             (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitFloat64              <T extends Float64>             (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitUtf8                 <T extends Utf8>                (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitBinary               <T extends Binary>              (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      arrayIndexOf(vector, index, value); }
    public visitFixedSizeBinary      <T extends FixedSizeBinary>     (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitDate                 <T extends Date_>               (vector: Vector<T>, value: T['TValue'] | null, index: number) { return       dateIndexOf(vector, index, value); }
    public visitDateDay              <T extends DateDay>             (vector: Vector<T>, value: T['TValue'] | null, index: number) { return       dateIndexOf(vector, index, value); }
    public visitDateMillisecond      <T extends DateMillisecond>     (vector: Vector<T>, value: T['TValue'] | null, index: number) { return       dateIndexOf(vector, index, value); }
    public visitTimestamp            <T extends Timestamp>           (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitTimestampSecond      <T extends TimestampSecond>     (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitTimestampMillisecond <T extends TimestampMillisecond>(vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitTimestampMicrosecond <T extends TimestampMicrosecond>(vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitTimestampNanosecond  <T extends TimestampNanosecond> (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitTime                 <T extends Time>                (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitTimeSecond           <T extends TimeSecond>          (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitTimeMillisecond      <T extends TimeMillisecond>     (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitTimeMicrosecond      <T extends TimeMicrosecond>     (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitTimeNanosecond       <T extends TimeNanosecond>      (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitDecimal              <T extends Decimal>             (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      arrayIndexOf(vector, index, value); }
    public visitList                 <T extends List>                (vector: Vector<T>, value: T['TValue'] | null, index: number) { return       listIndexOf(vector, index, value); }
    public visitStruct               <T extends Struct>              (vector: Vector<T>, value: T['TValue'] | null, index: number) { return     indexOfNested(vector, index, value); }
    public visitUnion                <T extends Union>               (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitDenseUnion           <T extends DenseUnion>          (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitSparseUnion          <T extends SparseUnion>         (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitDictionary           <T extends Dictionary>          (vector: Vector<T>, value: T['TValue'] | null, index: number) { return dictionaryIndexOf(vector, index, value); }
    public visitInterval             <T extends Interval>            (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitIntervalDayTime      <T extends IntervalDayTime>     (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitIntervalYearMonth    <T extends IntervalYearMonth>   (vector: Vector<T>, value: T['TValue'] | null, index: number) { return      valueIndexOf(vector, index, value); }
    public visitFixedSizeList        <T extends FixedSizeList>       (vector: Vector<T>, value: T['TValue'] | null, index: number) { return       listIndexOf(vector, index, value); }
    public visitMap                  <T extends Map_>                (vector: Vector<T>, value: T['TValue'] | null, index: number) { return     indexOfNested(vector, index, value); }
}

export const instance = new IndexOfVisitor();

function nullIndexOf(vector: Vector<Null>, fromIndex: number, searchElement: null) {
     // if you're looking for nulls and the vector isn't empty, we've got 'em!
    return searchElement === null && vector.length > 0 ? fromIndex : -1;
}

function indexOfNull<T extends DataType>(vector: Vector<T>, fromIndex: number): number {
    const { nullBitmap } = vector;
    if (!nullBitmap || vector.nullCount <= 0) {
        return -1;
    }
    let i = 0;
    for (const isValid of iterateBits(nullBitmap, vector.data.offset + fromIndex, vector.length, nullBitmap, getBool)) {
        if (!isValid) { return i; }
        ++i;
    }
    return -1;
}

function valueIndexOf<T extends DataType>(vector: Vector<T>, fromIndex: number, searchElement: T['TValue'] | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    for (let i = fromIndex - 1, n = vector.length; ++i < n;) {
        if (vector.get(i) === searchElement) {
            return i;
        }
    }
    return -1;
}

function dateIndexOf<T extends Date_>(vector: Vector<T>, fromIndex: number, searchElement: Date | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    const valueOfDate = searchElement.valueOf();
    for (let d: Date | null, i = fromIndex - 1, n = vector.length; ++i < n;) {
        if ((d = vector.get(i)) && d.valueOf() === valueOfDate) {
            return i;
        }
    }
    return -1;
}

function dictionaryIndexOf<T extends DataType>(vector: Vector<Dictionary<T>>, fromIndex: number, searchElement: T['TValue']): number {
    const { dictionary, indices } = vector;
    // First find the dictionary key for the desired value...
    const key = dictionary.indexOf(searchElement);
    // ... then find the first occurence of that key in indices
    return key === -1 ? -1 : indices.indexOf(key, fromIndex);
}

function arrayIndexOf<T extends DataType>(vector: Vector<T>, fromIndex: number, searchElement: T['TValue'] | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    searching:
    for (let x = null, j = 0, i = fromIndex - 1, n = vector.length, k = searchElement.length; ++i < n;) {
        if ((x = vector.get(i)) && (j = x.length) === k) {
            while (--j > -1) {
                if (x[j] !== searchElement[j]) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}

function listIndexOf<T extends DataType>(vector: Vector<List<T> | FixedSizeList<T>>, fromIndex: number, searchElement: Vector<T> | ArrayLike<T> | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    const getSearchElement = (Array.isArray(searchElement) || ArrayBuffer.isView(searchElement))
        ? (i: number) => (searchElement as ArrayLike<T>)[i]
        : (i: number) => (searchElement as Vector<T>).get(i);
    searching:
    for (let x = null, j = 0, i = fromIndex - 1, n = vector.length, k = searchElement.length; ++i < n;) {
        if ((x = vector.get(i)) && (j = x.length) === k) {
            while (--j > -1) {
                if (x.get(j) !== getSearchElement(j)) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}

function indexOfNested<T extends { [key: string]: DataType }>(vector: Vector<Map_<T> | Struct<T>>, fromIndex: number, searchElement: Row<T> | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    searching:
    for (let x = null, j = 0, i = fromIndex - 1, n = vector.length, k = searchElement.length; ++i < n;) {
        if ((x = vector.get(i)) && (j = x.length) === k) {
            while (--j > -1) {
                if (x[j] !== searchElement[j]) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}
