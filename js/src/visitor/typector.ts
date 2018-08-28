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
import { DataTypeCtor } from '../interfaces';
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

export interface GetDataTypeConstructor extends Visitor {
    visitMany <T extends Type>    (nodes: T[])                                              : DataTypeCtor<T>[];
    visit     <T extends Type>    (node: T,                                ...args: any[])  : DataTypeCtor<T>;
    getVisitFn<T extends Type>    (node: T)         : (typeId: T,          ...args: any[]) => DataTypeCtor<T>;
    getVisitFn<T extends DataType>(node: Vector<T>) : (typeId: T['TType'], ...args: any[]) => DataTypeCtor<T>;
    getVisitFn<T extends DataType>(node: Data<T>)   : (typeId: T['TType'], ...args: any[]) => DataTypeCtor<T>;
    getVisitFn<T extends DataType>(node: T)         : (typeId: T['TType'], ...args: any[]) => DataTypeCtor<T>;
}

export class GetDataTypeConstructor extends Visitor {
    public visitNull                 (_: Type.Null                 ) { return Null; }
    public visitBool                 (_: Type.Bool                 ) { return Bool; }
    public visitInt                  (_: Type.Int                  ) { return Int; }
    public visitInt8                 (_: Type.Int8                 ) { return Int8; }
    public visitInt16                (_: Type.Int16                ) { return Int16; }
    public visitInt32                (_: Type.Int32                ) { return Int32; }
    public visitInt64                (_: Type.Int64                ) { return Int64; }
    public visitUint8                (_: Type.Uint8                ) { return Uint8; }
    public visitUint16               (_: Type.Uint16               ) { return Uint16; }
    public visitUint32               (_: Type.Uint32               ) { return Uint32; }
    public visitUint64               (_: Type.Uint64               ) { return Uint64; }
    public visitFloat                (_: Type.Float                ) { return Float; }
    public visitFloat16              (_: Type.Float16              ) { return Float16; }
    public visitFloat32              (_: Type.Float32              ) { return Float32; }
    public visitFloat64              (_: Type.Float64              ) { return Float64; }
    public visitUtf8                 (_: Type.Utf8                 ) { return Utf8; }
    public visitBinary               (_: Type.Binary               ) { return Binary; }
    public visitFixedSizeBinary      (_: Type.FixedSizeBinary      ) { return FixedSizeBinary; }
    public visitDate                 (_: Type.Date                 ) { return Date_; }
    public visitDateDay              (_: Type.DateDay              ) { return DateDay; }
    public visitDateMillisecond      (_: Type.DateMillisecond      ) { return DateMillisecond; }
    public visitTimestamp            (_: Type.Timestamp            ) { return Timestamp; }
    public visitTimestampSecond      (_: Type.TimestampSecond      ) { return TimestampSecond; }
    public visitTimestampMillisecond (_: Type.TimestampMillisecond ) { return TimestampMillisecond; }
    public visitTimestampMicrosecond (_: Type.TimestampMicrosecond ) { return TimestampMicrosecond; }
    public visitTimestampNanosecond  (_: Type.TimestampNanosecond  ) { return TimestampNanosecond; }
    public visitTime                 (_: Type.Time                 ) { return Time; }
    public visitTimeSecond           (_: Type.TimeSecond           ) { return TimeSecond; }
    public visitTimeMillisecond      (_: Type.TimeMillisecond      ) { return TimeMillisecond; }
    public visitTimeMicrosecond      (_: Type.TimeMicrosecond      ) { return TimeMicrosecond; }
    public visitTimeNanosecond       (_: Type.TimeNanosecond       ) { return TimeNanosecond; }
    public visitDecimal              (_: Type.Decimal              ) { return Decimal; }
    public visitList                 (_: Type.List                 ) { return List; }
    public visitStruct               (_: Type.Struct               ) { return Struct; }
    public visitUnion                (_: Type.Union                ) { return Union; }
    public visitDenseUnion           (_: Type.DenseUnion           ) { return DenseUnion; }
    public visitSparseUnion          (_: Type.SparseUnion          ) { return SparseUnion; }
    public visitDictionary           (_: Type.Dictionary           ) { return Dictionary; }
    public visitInterval             (_: Type.Interval             ) { return Interval; }
    public visitIntervalDayTime      (_: Type.IntervalDayTime      ) { return IntervalDayTime; }
    public visitIntervalYearMonth    (_: Type.IntervalYearMonth    ) { return IntervalYearMonth; }
    public visitFixedSizeList        (_: Type.FixedSizeList        ) { return FixedSizeList; }
    public visitMap                  (_: Type.Map                  ) { return Map_; }
}

export const instance = new GetDataTypeConstructor();
