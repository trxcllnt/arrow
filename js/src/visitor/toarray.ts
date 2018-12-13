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
import { Type, Precision } from '../enum';
import { instance as iteratorVisitor } from './iterator';
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

export interface ToArrayVisitor extends Visitor {
    visitMany <T extends Vector>  (nodes: T[]     ): T['TArray'][];
    visit     <T extends Vector>  (node: T        ): T['TArray'];
    getVisitFn<T extends Type>    (node: T        ): (vector: Vector<T>) => Vector<T>['TArray'];
    getVisitFn<T extends DataType>(node: Vector<T>): (vector: Vector<T>) => Vector<T>['TArray'];
    getVisitFn<T extends DataType>(node: Data<T>  ): (vector: Vector<T>) => Vector<T>['TArray'];
    getVisitFn<T extends DataType>(node: T        ): (vector: Vector<T>) => Vector<T>['TArray'];
}

export class ToArrayVisitor extends Visitor {
    public visitNull                                    <T extends Null>                (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitBool                                    <T extends Bool>                (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitInt                                     <T extends Int>                 (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitInt8                                    <T extends Int8>                (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitInt16                                   <T extends Int16>               (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitInt32                                   <T extends Int32>               (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitInt64                                   <T extends Int64>               (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitUint8                                   <T extends Uint8>               (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitUint16                                  <T extends Uint16>              (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitUint32                                  <T extends Uint32>              (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitUint64                                  <T extends Uint64>              (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitFloat                                   <T extends Float>               (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitFloat16                                 <T extends Float16>             (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitFloat32                                 <T extends Float32>             (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitFloat64                                 <T extends Float64>             (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitUtf8                                    <T extends Utf8>                (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitBinary                                  <T extends Binary>              (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitFixedSizeBinary                         <T extends FixedSizeBinary>     (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitDate                                    <T extends Date_>               (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitDateDay                                 <T extends DateDay>             (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitDateMillisecond                         <T extends DateMillisecond>     (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTimestamp                               <T extends Timestamp>           (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTimestampSecond                         <T extends TimestampSecond>     (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTimestampMillisecond                    <T extends TimestampMillisecond>(vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTimestampMicrosecond                    <T extends TimestampMicrosecond>(vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTimestampNanosecond                     <T extends TimestampNanosecond> (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTime                                    <T extends Time>                (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTimeSecond                              <T extends TimeSecond>          (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTimeMillisecond                         <T extends TimeMillisecond>     (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTimeMicrosecond                         <T extends TimeMicrosecond>     (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitTimeNanosecond                          <T extends TimeNanosecond>      (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitDecimal                                 <T extends Decimal>             (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitList                <R extends DataType, T extends List<R>>             (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitStruct                                  <T extends Struct>              (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitUnion                                   <T extends Union>               (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitDenseUnion                              <T extends DenseUnion>          (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitSparseUnion                             <T extends SparseUnion>         (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitDictionary          <R extends DataType, T extends Dictionary<R>>       (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitInterval                                <T extends Interval>            (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitIntervalDayTime                         <T extends IntervalDayTime>     (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitIntervalYearMonth                       <T extends IntervalYearMonth>   (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitFixedSizeList       <R extends DataType, T extends FixedSizeList<R>>    (vector: Vector<T>) { return arrayOfVector(vector); }
    public visitMap                                     <T extends Map_>                (vector: Vector<T>) { return arrayOfVector(vector); }
}

export const instance = new ToArrayVisitor();

function arrayOfVector<T extends DataType>(vector: Vector<T>): T['TArray'] {

    const { type, length, stride } = vector;

    // Fast case, return subarray if possible
    switch (type.TType) {
        case Type.Int: case Type.Decimal:
        case Type.Time: case Type.Timestamp:
            return vector.values.subarray(0, length * stride);
        case Type.Float:
            return (type as Float).precision === Precision.HALF /* Precision.HALF */
                ? new Float32Array(vector[Symbol.iterator]())
                : vector.values.subarray(0, length * stride);
    }

    // Otherwise if not primitive, slow copy
    return [...iteratorVisitor.visit(vector)] as T['TArray'];
}

ToArrayVisitor.prototype.visitNull                 = arrayOfVector;
ToArrayVisitor.prototype.visitBool                 = arrayOfVector;
ToArrayVisitor.prototype.visitInt                  = arrayOfVector;
ToArrayVisitor.prototype.visitInt8                 = arrayOfVector;
ToArrayVisitor.prototype.visitInt16                = arrayOfVector;
ToArrayVisitor.prototype.visitInt32                = arrayOfVector;
ToArrayVisitor.prototype.visitInt64                = arrayOfVector;
ToArrayVisitor.prototype.visitUint8                = arrayOfVector;
ToArrayVisitor.prototype.visitUint16               = arrayOfVector;
ToArrayVisitor.prototype.visitUint32               = arrayOfVector;
ToArrayVisitor.prototype.visitUint64               = arrayOfVector;
ToArrayVisitor.prototype.visitFloat                = arrayOfVector;
ToArrayVisitor.prototype.visitFloat16              = arrayOfVector;
ToArrayVisitor.prototype.visitFloat32              = arrayOfVector;
ToArrayVisitor.prototype.visitFloat64              = arrayOfVector;
ToArrayVisitor.prototype.visitUtf8                 = arrayOfVector;
ToArrayVisitor.prototype.visitBinary               = arrayOfVector;
ToArrayVisitor.prototype.visitFixedSizeBinary      = arrayOfVector;
ToArrayVisitor.prototype.visitDate                 = arrayOfVector;
ToArrayVisitor.prototype.visitDateDay              = arrayOfVector;
ToArrayVisitor.prototype.visitDateMillisecond      = arrayOfVector;
ToArrayVisitor.prototype.visitTimestamp            = arrayOfVector;
ToArrayVisitor.prototype.visitTimestampSecond      = arrayOfVector;
ToArrayVisitor.prototype.visitTimestampMillisecond = arrayOfVector;
ToArrayVisitor.prototype.visitTimestampMicrosecond = arrayOfVector;
ToArrayVisitor.prototype.visitTimestampNanosecond  = arrayOfVector;
ToArrayVisitor.prototype.visitTime                 = arrayOfVector;
ToArrayVisitor.prototype.visitTimeSecond           = arrayOfVector;
ToArrayVisitor.prototype.visitTimeMillisecond      = arrayOfVector;
ToArrayVisitor.prototype.visitTimeMicrosecond      = arrayOfVector;
ToArrayVisitor.prototype.visitTimeNanosecond       = arrayOfVector;
ToArrayVisitor.prototype.visitDecimal              = arrayOfVector;
ToArrayVisitor.prototype.visitList                 = arrayOfVector;
ToArrayVisitor.prototype.visitStruct               = arrayOfVector;
ToArrayVisitor.prototype.visitUnion                = arrayOfVector;
ToArrayVisitor.prototype.visitDenseUnion           = arrayOfVector;
ToArrayVisitor.prototype.visitSparseUnion          = arrayOfVector;
ToArrayVisitor.prototype.visitDictionary           = arrayOfVector;
ToArrayVisitor.prototype.visitInterval             = arrayOfVector;
ToArrayVisitor.prototype.visitIntervalDayTime      = arrayOfVector;
ToArrayVisitor.prototype.visitIntervalYearMonth    = arrayOfVector;
ToArrayVisitor.prototype.visitFixedSizeList        = arrayOfVector;
ToArrayVisitor.prototype.visitMap                  = arrayOfVector;
