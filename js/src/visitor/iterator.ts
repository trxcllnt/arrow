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
import { iterateBits } from '../util/bit';
import { instance as getVisitor } from './get';
import { Vector } from '../interfaces';
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

export interface IteratorVisitor extends Visitor {
    visitMany <T extends Vector>  (nodes: T[]     ): IterableIterator<T['TValue'] | null>[];
    visit     <T extends Vector>  (node: T        ): IterableIterator<T['TValue'] | null>;
    getVisitFn<T extends Type>    (node: T        ): (vector: Vector<T>) => IterableIterator<Vector<T>['TValue'] | null>;
    getVisitFn<T extends DataType>(node: Vector<T>): (vector: Vector<T>) => IterableIterator<Vector<T>['TValue'] | null>;
    getVisitFn<T extends DataType>(node: Data<T>  ): (vector: Vector<T>) => IterableIterator<Vector<T>['TValue'] | null>;
    getVisitFn<T extends DataType>(node: T        ): (vector: Vector<T>) => IterableIterator<Vector<T>['TValue'] | null>;
}

export class IteratorVisitor extends Visitor {
    public visitNull                 <T extends Null>                (vector: Vector<T>) { return vectorIterator(vector); }
    public visitBool                 <T extends Bool>                (vector: Vector<T>) { return vectorIterator(vector); }
    public visitInt                  <T extends Int>                 (vector: Vector<T>) { return vectorIterator(vector); }
    public visitInt8                 <T extends Int8>                (vector: Vector<T>) { return vectorIterator(vector); }
    public visitInt16                <T extends Int16>               (vector: Vector<T>) { return vectorIterator(vector); }
    public visitInt32                <T extends Int32>               (vector: Vector<T>) { return vectorIterator(vector); }
    public visitInt64                <T extends Int64>               (vector: Vector<T>) { return vectorIterator(vector); }
    public visitUint8                <T extends Uint8>               (vector: Vector<T>) { return vectorIterator(vector); }
    public visitUint16               <T extends Uint16>              (vector: Vector<T>) { return vectorIterator(vector); }
    public visitUint32               <T extends Uint32>              (vector: Vector<T>) { return vectorIterator(vector); }
    public visitUint64               <T extends Uint64>              (vector: Vector<T>) { return vectorIterator(vector); }
    public visitFloat                <T extends Float>               (vector: Vector<T>) { return vectorIterator(vector); }
    public visitFloat16              <T extends Float16>             (vector: Vector<T>) { return vectorIterator(vector); }
    public visitFloat32              <T extends Float32>             (vector: Vector<T>) { return vectorIterator(vector); }
    public visitFloat64              <T extends Float64>             (vector: Vector<T>) { return vectorIterator(vector); }
    public visitUtf8                 <T extends Utf8>                (vector: Vector<T>) { return vectorIterator(vector); }
    public visitBinary               <T extends Binary>              (vector: Vector<T>) { return vectorIterator(vector); }
    public visitFixedSizeBinary      <T extends FixedSizeBinary>     (vector: Vector<T>) { return vectorIterator(vector); }
    public visitDate                 <T extends Date_>               (vector: Vector<T>) { return vectorIterator(vector); }
    public visitDateDay              <T extends DateDay>             (vector: Vector<T>) { return vectorIterator(vector); }
    public visitDateMillisecond      <T extends DateMillisecond>     (vector: Vector<T>) { return vectorIterator(vector); }
    public visitTimestamp            <T extends Timestamp>           (vector: Vector<T>) { return vectorIterator(vector); }
    public visitTimestampSecond      <T extends TimestampSecond>     (vector: Vector<T>) { return vectorIterator(vector); }
    public visitTimestampMillisecond <T extends TimestampMillisecond>(vector: Vector<T>) { return vectorIterator(vector); }
    public visitTimestampMicrosecond <T extends TimestampMicrosecond>(vector: Vector<T>) { return vectorIterator(vector); }
    public visitTimestampNanosecond  <T extends TimestampNanosecond> (vector: Vector<T>) { return vectorIterator(vector); }
    public visitTime                 <T extends Time>                (vector: Vector<T>) { return vectorIterator(vector); }
    public visitTimeSecond           <T extends TimeSecond>          (vector: Vector<T>) { return vectorIterator(vector); }
    public visitTimeMillisecond      <T extends TimeMillisecond>     (vector: Vector<T>) { return vectorIterator(vector); }
    public visitTimeMicrosecond      <T extends TimeMicrosecond>     (vector: Vector<T>) { return vectorIterator(vector); }
    public visitTimeNanosecond       <T extends TimeNanosecond>      (vector: Vector<T>) { return vectorIterator(vector); }
    public visitDecimal              <T extends Decimal>             (vector: Vector<T>) { return vectorIterator(vector); }
    public visitList                 <T extends List>                (vector: Vector<T>) { return vectorIterator(vector); }
    public visitStruct               <T extends Struct>              (vector: Vector<T>) { return vectorIterator(vector); }
    public visitUnion                <T extends Union>               (vector: Vector<T>) { return vectorIterator(vector); }
    public visitDenseUnion           <T extends DenseUnion>          (vector: Vector<T>) { return vectorIterator(vector); }
    public visitSparseUnion          <T extends SparseUnion>         (vector: Vector<T>) { return vectorIterator(vector); }
    public visitDictionary           <T extends Dictionary>          (vector: Vector<T>) { return vectorIterator(vector); }
    public visitInterval             <T extends Interval>            (vector: Vector<T>) { return vectorIterator(vector); }
    public visitIntervalDayTime      <T extends IntervalDayTime>     (vector: Vector<T>) { return vectorIterator(vector); }
    public visitIntervalYearMonth    <T extends IntervalYearMonth>   (vector: Vector<T>) { return vectorIterator(vector); }
    public visitFixedSizeList        <T extends FixedSizeList>       (vector: Vector<T>) { return vectorIterator(vector); }
    public visitMap                  <T extends Map_>                (vector: Vector<T>) { return vectorIterator(vector); }
}

export const instance = new IteratorVisitor();

function nullableIterator<T extends DataType>(vector: Vector<T>): IterableIterator<T['TValue'] | null> {
    const getFn = getVisitor.getVisitFn(vector);
    return iterateBits<T['TValue'] | null>(
        vector.nullBitmap, vector.offset, vector.length, vector,
        (vec: Vector<T>, idx: number, nullByte: number, nullBit: number) =>
            ((nullByte & 1 << nullBit) !== 0) ? getFn(vec, idx) : null
    );
}

function vectorIterator<T extends DataType>(vector: Vector<T>): IterableIterator<T['TValue'] | null> {

    // If nullable, iterate manually
    if (vector.nullCount > 0) {
        return nullableIterator<T>(vector);
    }

    const { type, length } = vector;

    // Fast case, defer to native iterators if possible
    if (vector.stride === 1 && (
        (type.TType === Type.Timestamp) ||
        (type.TType === Type.Int && (type as Int).bitWidth !== 64) ||
        (type.TType === Type.Time && (type as Time).bitWidth !== 64) ||
        (type.TType === Type.Float && (type as Float).precision > 0 /* Precision.HALF */)
    )) {
        return vector.values.subarray(0, length);
    }

    // Otherwise, iterate manually
    return (function* (getFn) {
        for (let index = -1; ++index < length;) {
            yield getFn(vector, index);
        }
    })(getVisitor.getVisitFn(vector));
}
