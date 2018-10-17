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
import * as vecs from '../vector';
import { DataType } from '../type';
import { Visitor } from '../visitor';
import { Vector, VectorCtor } from '../interfaces';

export interface GetVectorConstructor extends Visitor {
    visitMany <T extends Type>    (nodes: T[]     ): VectorCtor<T>[];
    visit     <T extends Type>    (node: T,       ): VectorCtor<T>;
    getVisitFn<T extends Type>    (node: T        ): () => VectorCtor<T>;
    getVisitFn<T extends DataType>(node: Vector<T>): () => VectorCtor<T>;
    getVisitFn<T extends DataType>(node: Data<T>  ): () => VectorCtor<T>;
    getVisitFn<T extends DataType>(node: T        ): () => VectorCtor<T>;
}

export class GetVectorConstructor extends Visitor {
    public visitNull                 () { return vecs.NullVector; }
    public visitBool                 () { return vecs.BoolVector; }
    public visitInt                  () { return vecs.IntVector; }
    public visitInt8                 () { return vecs.Int8Vector; }
    public visitInt16                () { return vecs.Int16Vector; }
    public visitInt32                () { return vecs.Int32Vector; }
    public visitInt64                () { return vecs.Int64Vector; }
    public visitUint8                () { return vecs.Uint8Vector; }
    public visitUint16               () { return vecs.Uint16Vector; }
    public visitUint32               () { return vecs.Uint32Vector; }
    public visitUint64               () { return vecs.Uint64Vector; }
    public visitFloat                () { return vecs.FloatVector; }
    public visitFloat16              () { return vecs.Float16Vector; }
    public visitFloat32              () { return vecs.Float32Vector; }
    public visitFloat64              () { return vecs.Float64Vector; }
    public visitUtf8                 () { return vecs.Utf8Vector; }
    public visitBinary               () { return vecs.BinaryVector; }
    public visitFixedSizeBinary      () { return vecs.FixedSizeBinaryVector; }
    public visitDate                 () { return vecs.DateVector; }
    public visitDateDay              () { return vecs.DateDayVector; }
    public visitDateMillisecond      () { return vecs.DateMillisecondVector; }
    public visitTimestamp            () { return vecs.TimestampVector; }
    public visitTimestampSecond      () { return vecs.TimestampSecondVector; }
    public visitTimestampMillisecond () { return vecs.TimestampMillisecondVector; }
    public visitTimestampMicrosecond () { return vecs.TimestampMicrosecondVector; }
    public visitTimestampNanosecond  () { return vecs.TimestampNanosecondVector; }
    public visitTime                 () { return vecs.TimeVector; }
    public visitTimeSecond           () { return vecs.TimeSecondVector; }
    public visitTimeMillisecond      () { return vecs.TimeMillisecondVector; }
    public visitTimeMicrosecond      () { return vecs.TimeMicrosecondVector; }
    public visitTimeNanosecond       () { return vecs.TimeNanosecondVector; }
    public visitDecimal              () { return vecs.DecimalVector; }
    public visitList                 () { return vecs.ListVector; }
    public visitStruct               () { return vecs.StructVector; }
    public visitUnion                () { return vecs.UnionVector; }
    public visitDenseUnion           () { return vecs.DenseUnionVector; }
    public visitSparseUnion          () { return vecs.SparseUnionVector; }
    public visitDictionary           () { return vecs.DictionaryVector; }
    public visitInterval             () { return vecs.IntervalVector; }
    public visitIntervalDayTime      () { return vecs.IntervalDayTimeVector; }
    public visitIntervalYearMonth    () { return vecs.IntervalYearMonthVector; }
    public visitFixedSizeList        () { return vecs.FixedSizeListVector; }
    public visitMap                  () { return vecs.MapVector; }
}

export const instance = new GetVectorConstructor();
