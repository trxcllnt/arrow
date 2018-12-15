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

import { RowLike } from '../type';
import { Data } from '../data';
import { Type } from '../enum';
import { Visitor } from '../visitor';
import { Vector } from '../interfaces';
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
    visitMany <T extends Vector>  (nodes: T[], values: (T['TValue'] | null)[], indices: (number | undefined)[]): number[];
    visit     <T extends Vector>  (node: T, value: T['TValue'] | null, index?: number            ): number;
    getVisitFn<T extends Type>    (node: T         ): (vector: Vector<T>, value: Vector<T>['TValue'] | null, index?: number) => number;
    getVisitFn<T extends DataType>(node: Vector<T> ): (vector: Vector<T>, value:         T['TValue'] | null, index?: number) => number;
    getVisitFn<T extends DataType>(node: Data<T>   ): (vector: Vector<T>, value:         T['TValue'] | null, index?: number) => number;
    getVisitFn<T extends DataType>(node: T         ): (vector: Vector<T>, value:         T['TValue'] | null, index?: number) => number;
}

export class IndexOfVisitor extends Visitor {
    public visitNull                 <T extends Null>                (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return       nullIndexOf(vector, value, index || 0); }
    public visitBool                 <T extends Bool>                (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitInt                  <T extends Int>                 (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitInt8                 <T extends Int8>                (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitInt16                <T extends Int16>               (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitInt32                <T extends Int32>               (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitInt64                <T extends Int64>               (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      arrayIndexOf(vector, value, index || 0); }
    public visitUint8                <T extends Uint8>               (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitUint16               <T extends Uint16>              (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitUint32               <T extends Uint32>              (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitUint64               <T extends Uint64>              (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      arrayIndexOf(vector, value, index || 0); }
    public visitFloat                <T extends Float>               (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitFloat16              <T extends Float16>             (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitFloat32              <T extends Float32>             (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitFloat64              <T extends Float64>             (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitUtf8                 <T extends Utf8>                (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitBinary               <T extends Binary>              (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      arrayIndexOf(vector, value, index || 0); }
    public visitFixedSizeBinary      <T extends FixedSizeBinary>     (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      arrayIndexOf(vector, value, index || 0); }
    public visitDate                 <T extends Date_>               (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return       dateIndexOf(vector, value, index || 0); }
    public visitDateDay              <T extends DateDay>             (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return       dateIndexOf(vector, value, index || 0); }
    public visitDateMillisecond      <T extends DateMillisecond>     (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return       dateIndexOf(vector, value, index || 0); }
    public visitTimestamp            <T extends Timestamp>           (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitTimestampSecond      <T extends TimestampSecond>     (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitTimestampMillisecond <T extends TimestampMillisecond>(vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitTimestampMicrosecond <T extends TimestampMicrosecond>(vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitTimestampNanosecond  <T extends TimestampNanosecond> (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitTime                 <T extends Time>                (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitTimeSecond           <T extends TimeSecond>          (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitTimeMillisecond      <T extends TimeMillisecond>     (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitTimeMicrosecond      <T extends TimeMicrosecond>     (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitTimeNanosecond       <T extends TimeNanosecond>      (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitDecimal              <T extends Decimal>             (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      arrayIndexOf(vector, value, index || 0); }
    public visitList                 <T extends List>                (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return       listIndexOf(vector, value, index || 0); }
    public visitStruct               <T extends Struct>              (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return     indexOfNested(vector, value, index || 0); }
    public visitUnion                <T extends Union>               (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitDenseUnion           <T extends DenseUnion>          (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitSparseUnion          <T extends SparseUnion>         (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitDictionary           <T extends Dictionary>          (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return dictionaryIndexOf(vector, value, index || 0); }
    public visitInterval             <T extends Interval>            (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitIntervalDayTime      <T extends IntervalDayTime>     (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitIntervalYearMonth    <T extends IntervalYearMonth>   (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return      valueIndexOf(vector, value, index || 0); }
    public visitFixedSizeList        <T extends FixedSizeList>       (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return       listIndexOf(vector, value, index || 0); }
    public visitMap                  <T extends Map_>                (vector: Vector<T>, value: T['TValue'] | null, index?: number) { return     indexOfNested(vector, value, index || 0); }
}

export const instance = new IndexOfVisitor();

function nullIndexOf(vector: Vector<Null>, searchElement?: null, fromIndex?: number) {
     // if you're looking for nulls and the vector isn't empty, we've got 'em!
    return searchElement === null && vector.length > 0 ? fromIndex : -1;
}

function indexOfNull<T extends DataType>(vector: Vector<T>, fromIndex?: number): number {
    const { nullBitmap } = vector;
    if (!nullBitmap || vector.nullCount <= 0) {
        return -1;
    }
    let i = 0;
    for (const isValid of iterateBits(nullBitmap, vector.data.offset + (fromIndex || 0), vector.length, nullBitmap, getBool)) {
        if (!isValid) { return i; }
        ++i;
    }
    return -1;
}

function valueIndexOf<T extends DataType>(vector: Vector<T>, searchElement?: T['TValue'] | null, fromIndex?: number): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    for (let i = (fromIndex || 0) - 1, n = vector.length; ++i < n;) {
        if (vector.get(i) === searchElement) {
            return i;
        }
    }
    return -1;
}

function dateIndexOf<T extends Date_>(vector: Vector<T>, searchElement?: Date | null, fromIndex?: number): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    const valueOfDate = searchElement.valueOf();
    for (let d: Date | null, i = (fromIndex || 0) - 1, n = vector.length; ++i < n;) {
        if ((d = vector.get(i)) && d.valueOf() === valueOfDate) {
            return i;
        }
    }
    return -1;
}

function dictionaryIndexOf<T extends DataType>(vector: Vector<Dictionary<T>>, searchElement?: T['TValue'] | null, fromIndex?: number): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    const { dictionary, indices } = vector;
    // First find the dictionary key for the desired value...
    const key = dictionary.indexOf(searchElement);
    // ... then find the first occurence of that key in indices
    return key === -1 ? -1 : indices.indexOf(key, fromIndex);
}

function arrayIndexOf<T extends DataType>(vector: Vector<T>, searchElement?: T['TValue'] | null, fromIndex?: number): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    searching:
    for (let x = null, j = 0, i = (fromIndex || 0) - 1, n = vector.length, k = searchElement.length; ++i < n;) {
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

function listIndexOf<
    T extends DataType,
    R extends List<T> | FixedSizeList<T>
>(vector: Vector<R>, searchElement?: R['TValue'] | null, fromIndex?: number): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    const getSearchElement = (Array.isArray(searchElement) || ArrayBuffer.isView(searchElement))
        ? (i: number) => (searchElement as ArrayLike<T>)[i]
        : (i: number) => (searchElement as Vector<T>).get(i);
    searching:
    for (let x = null, j = 0, i = (fromIndex || 0) - 1, n = vector.length, k = searchElement.length; ++i < n;) {
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

function indexOfNested<
    T extends { [key: string]: DataType },
    R extends Map_<T> | Struct<T>
>(vector: Vector<R>, searchElement?: RowLike<T> | null, fromIndex?: number): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    searching:
    for (let x = null, j = 0, i = (fromIndex || 0) - 1, n = vector.length, k = searchElement.length; ++i < n;) {
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

IndexOfVisitor.prototype.visitNull                 =       nullIndexOf;
IndexOfVisitor.prototype.visitBool                 =      valueIndexOf;
IndexOfVisitor.prototype.visitInt                  =      valueIndexOf;
IndexOfVisitor.prototype.visitInt8                 =      valueIndexOf;
IndexOfVisitor.prototype.visitInt16                =      valueIndexOf;
IndexOfVisitor.prototype.visitInt32                =      valueIndexOf;
IndexOfVisitor.prototype.visitInt64                =      arrayIndexOf;
IndexOfVisitor.prototype.visitUint8                =      valueIndexOf;
IndexOfVisitor.prototype.visitUint16               =      valueIndexOf;
IndexOfVisitor.prototype.visitUint32               =      valueIndexOf;
IndexOfVisitor.prototype.visitUint64               =      arrayIndexOf;
IndexOfVisitor.prototype.visitFloat                =      valueIndexOf;
IndexOfVisitor.prototype.visitFloat16              =      valueIndexOf;
IndexOfVisitor.prototype.visitFloat32              =      valueIndexOf;
IndexOfVisitor.prototype.visitFloat64              =      valueIndexOf;
IndexOfVisitor.prototype.visitUtf8                 =      valueIndexOf;
IndexOfVisitor.prototype.visitBinary               =      arrayIndexOf;
IndexOfVisitor.prototype.visitFixedSizeBinary      =      arrayIndexOf;
IndexOfVisitor.prototype.visitDate                 =       dateIndexOf;
IndexOfVisitor.prototype.visitDateDay              =       dateIndexOf;
IndexOfVisitor.prototype.visitDateMillisecond      =       dateIndexOf;
IndexOfVisitor.prototype.visitTimestamp            =      valueIndexOf;
IndexOfVisitor.prototype.visitTimestampSecond      =      valueIndexOf;
IndexOfVisitor.prototype.visitTimestampMillisecond =      valueIndexOf;
IndexOfVisitor.prototype.visitTimestampMicrosecond =      valueIndexOf;
IndexOfVisitor.prototype.visitTimestampNanosecond  =      valueIndexOf;
IndexOfVisitor.prototype.visitTime                 =      valueIndexOf;
IndexOfVisitor.prototype.visitTimeSecond           =      valueIndexOf;
IndexOfVisitor.prototype.visitTimeMillisecond      =      valueIndexOf;
IndexOfVisitor.prototype.visitTimeMicrosecond      =      valueIndexOf;
IndexOfVisitor.prototype.visitTimeNanosecond       =      valueIndexOf;
IndexOfVisitor.prototype.visitDecimal              =      arrayIndexOf;
IndexOfVisitor.prototype.visitList                 =       listIndexOf;
IndexOfVisitor.prototype.visitStruct               =     indexOfNested;
IndexOfVisitor.prototype.visitUnion                =      valueIndexOf;
IndexOfVisitor.prototype.visitDenseUnion           =      valueIndexOf;
IndexOfVisitor.prototype.visitSparseUnion          =      valueIndexOf;
IndexOfVisitor.prototype.visitDictionary           = dictionaryIndexOf;
IndexOfVisitor.prototype.visitInterval             =      valueIndexOf;
IndexOfVisitor.prototype.visitIntervalDayTime      =      valueIndexOf;
IndexOfVisitor.prototype.visitIntervalYearMonth    =      valueIndexOf;
IndexOfVisitor.prototype.visitFixedSizeList        =       listIndexOf;
IndexOfVisitor.prototype.visitMap                  =     indexOfNested;
