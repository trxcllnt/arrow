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
import { DataType } from '../type';
import { Visitor } from '../visitor';
import { Vector, VectorCtor } from '../interfaces';
import {
    NullVector, BoolVector, DictionaryVector,
    IntVector, Int8Vector, Int16Vector, Int32Vector, Int64Vector,
    Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector,
    FloatVector, Float16Vector, Float32Vector, Float64Vector, DecimalVector,
    Utf8Vector, BinaryVector, FixedSizeBinaryVector,
    ListVector, FixedSizeListVector,
    DateVector, DateDayVector, DateMillisecondVector,
    TimeVector, TimeSecondVector, TimeMillisecondVector, TimeMicrosecondVector, TimeNanosecondVector,
    IntervalVector, IntervalDayTimeVector, IntervalYearMonthVector,
    TimestampVector, TimestampSecondVector, TimestampMillisecondVector, TimestampMicrosecondVector, TimestampNanosecondVector,
    StructVector, MapVector, UnionVector, DenseUnionVector, SparseUnionVector,
} from '../vector';

export interface GetVectorConstructor extends Visitor {
    visitMany<T extends Type>(nodes: T[]): VectorCtor<T>[];
    visit<T extends Type>(node: T, ...args: any[]): VectorCtor<T>;
    getVisitFn<T extends Type>(node: T): (typeId: T, ...args: any[]) => VectorCtor<T>;
    getVisitFn<T extends DataType>(node: T): (typeId: T['TType'], ...args: any[]) => VectorCtor<T>;
    getVisitFn<T extends DataType>(node: Data<T>): (typeId: T['TType'], ...args: any[]) => VectorCtor<T>;
    getVisitFn<T extends DataType>(node: Vector<T>): (typeId: T['TType'], ...args: any[]) => VectorCtor<T>;
}

export class GetVectorConstructor extends Visitor {
    public visitNull                 (_: Type.Null                 ) { return NullVector; }
    public visitBool                 (_: Type.Bool                 ) { return BoolVector; }
    public visitInt                  (_: Type.Int                  ) { return IntVector; }
    public visitInt8                 (_: Type.Int8                 ) { return Int8Vector; }
    public visitInt16                (_: Type.Int16                ) { return Int16Vector; }
    public visitInt32                (_: Type.Int32                ) { return Int32Vector; }
    public visitInt64                (_: Type.Int64                ) { return Int64Vector; }
    public visitUint8                (_: Type.Uint8                ) { return Uint8Vector; }
    public visitUint16               (_: Type.Uint16               ) { return Uint16Vector; }
    public visitUint32               (_: Type.Uint32               ) { return Uint32Vector; }
    public visitUint64               (_: Type.Uint64               ) { return Uint64Vector; }
    public visitFloat                (_: Type.Float                ) { return FloatVector; }
    public visitFloat16              (_: Type.Float16              ) { return Float16Vector; }
    public visitFloat32              (_: Type.Float32              ) { return Float32Vector; }
    public visitFloat64              (_: Type.Float64              ) { return Float64Vector; }
    public visitUtf8                 (_: Type.Utf8                 ) { return Utf8Vector; }
    public visitBinary               (_: Type.Binary               ) { return BinaryVector; }
    public visitFixedSizeBinary      (_: Type.FixedSizeBinary      ) { return FixedSizeBinaryVector; }
    public visitDate                 (_: Type.Date                 ) { return DateVector; }
    public visitDateDay              (_: Type.DateDay              ) { return DateDayVector; }
    public visitDateMillisecond      (_: Type.DateMillisecond      ) { return DateMillisecondVector; }
    public visitTimestamp            (_: Type.Timestamp            ) { return TimestampVector; }
    public visitTimestampSecond      (_: Type.TimestampSecond      ) { return TimestampSecondVector; }
    public visitTimestampMillisecond (_: Type.TimestampMillisecond ) { return TimestampMillisecondVector; }
    public visitTimestampMicrosecond (_: Type.TimestampMicrosecond ) { return TimestampMicrosecondVector; }
    public visitTimestampNanosecond  (_: Type.TimestampNanosecond  ) { return TimestampNanosecondVector; }
    public visitTime                 (_: Type.Time                 ) { return TimeVector; }
    public visitTimeSecond           (_: Type.TimeSecond           ) { return TimeSecondVector; }
    public visitTimeMillisecond      (_: Type.TimeMillisecond      ) { return TimeMillisecondVector; }
    public visitTimeMicrosecond      (_: Type.TimeMicrosecond      ) { return TimeMicrosecondVector; }
    public visitTimeNanosecond       (_: Type.TimeNanosecond       ) { return TimeNanosecondVector; }
    public visitDecimal              (_: Type.Decimal              ) { return DecimalVector; }
    public visitList                 (_: Type.List                 ) { return ListVector; }
    public visitStruct               (_: Type.Struct               ) { return StructVector; }
    public visitUnion                (_: Type.Union                ) { return UnionVector; }
    public visitDenseUnion           (_: Type.DenseUnion           ) { return DenseUnionVector; }
    public visitSparseUnion          (_: Type.SparseUnion          ) { return SparseUnionVector; }
    public visitDictionary           (_: Type.Dictionary           ) { return DictionaryVector; }
    public visitInterval             (_: Type.Interval             ) { return IntervalVector; }
    public visitIntervalDayTime      (_: Type.IntervalDayTime      ) { return IntervalDayTimeVector; }
    public visitIntervalYearMonth    (_: Type.IntervalYearMonth    ) { return IntervalYearMonthVector; }
    public visitFixedSizeList        (_: Type.FixedSizeList        ) { return FixedSizeListVector; }
    public visitMap                  (_: Type.Map                  ) { return MapVector; }
}

export const instance = new GetVectorConstructor();
