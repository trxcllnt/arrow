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

import { Data } from './data';
import { Vector } from './vector';
import {
    Type, DataType, Dictionary,
    Bool, Null, Utf8, Binary, Decimal, FixedSizeBinary, List, FixedSizeList, Map_, Struct,

    Float, Float16, Float32, Float64,
    Int, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64,
    Date_, DateDay, DateMillisecond,
    Interval, IntervalDayTime, IntervalYearMonth,
    Time, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    Timestamp, TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Union, DenseUnion, SparseUnion,

    Precision, DateUnit, TimeUnit, IntervalUnit, UnionMode,
} from './type';

enum VisitorKind { Type, Data, Vector, DataType };
type VisitResult = { [T in Type]: any };
type VisitorNode<T extends DataType> = {
    [VisitorKind.Type]: T['TType'];
    [VisitorKind.Data]: Data<T>;
    [VisitorKind.Vector]: Vector<T>;
    [VisitorKind.DataType]: T;
};

export interface Visitor<K extends VisitorKind, TResult extends VisitResult = VisitResult> {
    visitNull                                    (node: VisitorNode<Null>                [K], ...args: any[]): TResult[Type.Null];
    visitBool                                    (node: VisitorNode<Bool>                [K], ...args: any[]): TResult[Type.Bool];
    visitInt                                     (node: VisitorNode<Int>                 [K], ...args: any[]): TResult[Type.Int];
    visitInt8?                                   (node: VisitorNode<Int8>                [K], ...args: any[]): TResult[Type.Int8];
    visitInt16?                                  (node: VisitorNode<Int16>               [K], ...args: any[]): TResult[Type.Int16];
    visitInt32?                                  (node: VisitorNode<Int32>               [K], ...args: any[]): TResult[Type.Int32];
    visitInt64?                                  (node: VisitorNode<Int64>               [K], ...args: any[]): TResult[Type.Int64];
    visitUint8?                                  (node: VisitorNode<Uint8>               [K], ...args: any[]): TResult[Type.Uint8];
    visitUint16?                                 (node: VisitorNode<Uint16>              [K], ...args: any[]): TResult[Type.Uint16];
    visitUint32?                                 (node: VisitorNode<Uint32>              [K], ...args: any[]): TResult[Type.Uint32];
    visitUint64?                                 (node: VisitorNode<Uint64>              [K], ...args: any[]): TResult[Type.Uint64];
    visitFloat                                   (node: VisitorNode<Float>               [K], ...args: any[]): TResult[Type.Float];
    visitFloat16?                                (node: VisitorNode<Float16>             [K], ...args: any[]): TResult[Type.Float16];
    visitFloat32?                                (node: VisitorNode<Float32>             [K], ...args: any[]): TResult[Type.Float32];
    visitFloat64?                                (node: VisitorNode<Float64>             [K], ...args: any[]): TResult[Type.Float64];
    visitUtf8                                    (node: VisitorNode<Utf8>                [K], ...args: any[]): TResult[Type.Utf8];
    visitBinary                                  (node: VisitorNode<Binary>              [K], ...args: any[]): TResult[Type.Binary];
    visitFixedSizeBinary                         (node: VisitorNode<FixedSizeBinary>     [K], ...args: any[]): TResult[Type.FixedSizeBinary];
    visitDate                                    (node: VisitorNode<Date_>               [K], ...args: any[]): TResult[Type.Date];
    visitDateDay?                                (node: VisitorNode<DateDay>             [K], ...args: any[]): TResult[Type.DateDay];
    visitDateMillisecond?                        (node: VisitorNode<DateMillisecond>     [K], ...args: any[]): TResult[Type.DateMillisecond];
    visitTimestamp                               (node: VisitorNode<Timestamp>           [K], ...args: any[]): TResult[Type.Timestamp];
    visitTimestampSecond?                        (node: VisitorNode<TimestampSecond>     [K], ...args: any[]): TResult[Type.TimestampSecond];
    visitTimestampMillisecond?                   (node: VisitorNode<TimestampMillisecond>[K], ...args: any[]): TResult[Type.TimestampMillisecond];
    visitTimestampMicrosecond?                   (node: VisitorNode<TimestampMicrosecond>[K], ...args: any[]): TResult[Type.TimestampMicrosecond];
    visitTimestampNanosecond?                    (node: VisitorNode<TimestampNanosecond> [K], ...args: any[]): TResult[Type.TimestampNanosecond];
    visitTime                                    (node: VisitorNode<Time>                [K], ...args: any[]): TResult[Type.Time];
    visitTimeSecond?                             (node: VisitorNode<TimeSecond>          [K], ...args: any[]): TResult[Type.TimeSecond];
    visitTimeMillisecond?                        (node: VisitorNode<TimeMillisecond>     [K], ...args: any[]): TResult[Type.TimeMillisecond];
    visitTimeMicrosecond?                        (node: VisitorNode<TimeMicrosecond>     [K], ...args: any[]): TResult[Type.TimeMicrosecond];
    visitTimeNanosecond?                         (node: VisitorNode<TimeNanosecond>      [K], ...args: any[]): TResult[Type.TimeNanosecond];
    visitDecimal                                 (node: VisitorNode<Decimal>             [K], ...args: any[]): TResult[Type.Decimal];
    visitList                <T extends DataType>(node: VisitorNode<List<T>>             [K], ...args: any[]): TResult[Type.List];
    visitStruct                                  (node: VisitorNode<Struct>              [K], ...args: any[]): TResult[Type.Struct];
    visitUnion                                   (node: VisitorNode<Union>               [K], ...args: any[]): TResult[Type.Union];
    visitDenseUnion?                             (node: VisitorNode<DenseUnion>          [K], ...args: any[]): TResult[Type.DenseUnion];
    visitSparseUnion?                            (node: VisitorNode<SparseUnion>         [K], ...args: any[]): TResult[Type.SparseUnion];
    visitDictionary          <T extends DataType>(node: VisitorNode<Dictionary<T>>       [K], ...args: any[]): TResult[Type.Dictionary];
    visitInterval                                (node: VisitorNode<Interval>            [K], ...args: any[]): TResult[Type.Interval];
    visitIntervalDayTime?                        (node: VisitorNode<IntervalDayTime>     [K], ...args: any[]): TResult[Type.IntervalDayTime];
    visitIntervalYearMonth?                      (node: VisitorNode<IntervalYearMonth>   [K], ...args: any[]): TResult[Type.IntervalYearMonth];
    visitFixedSizeList       <T extends DataType>(node: VisitorNode<FixedSizeList<T>>    [K], ...args: any[]): TResult[Type.FixedSizeList];
    visitMap                                     (node: VisitorNode<Map_>                [K], ...args: any[]): TResult[Type.Map];
}

export abstract class Visitor<K extends VisitorKind, TResult extends VisitResult = VisitResult> {
    public visitMany<T extends DataType>(nodes: VisitorNode<T>[K][]) { return nodes.map((node) => this.visit(node)); }
    public visit<T extends DataType>(node: VisitorNode<T>[K], ...args: any[]) {
        type TVisit = (_: VisitorNode<T>[K], ...args: any[]) => TResult[T['TType']];
        const visit: TVisit = getVisitFn(this, node, false).bind(this);
        return visit ? visit(node, ...args) : null;
    }
    public getVisitFn<T extends DataType>(node: VisitorNode<T>[K]) {
        type TVisit = (_: VisitorNode<T>[K], ...args: any[]) => TResult[T['TType']];
        const visit: TVisit = getVisitFn(this, node, true).bind(this);
        return visit;
    }
}

export abstract class TypeVisitor<TResult extends VisitResult = VisitResult> extends Visitor<VisitorKind.Type, TResult> {}
export abstract class DataVisitor<TResult extends VisitResult = VisitResult> extends Visitor<VisitorKind.Data, TResult> {}
export abstract class VectorVisitor<TResult extends VisitResult = VisitResult> extends Visitor<VisitorKind.Vector, TResult> {}
export abstract class DataTypeVisitor<TResult extends VisitResult = VisitResult> extends Visitor<VisitorKind.DataType, TResult> {}

function inferDType<T extends DataType>(type: T): Type {
    switch (type.TType) {
        case Type.Null: return Type.Null;
        case Type.Int:
            const { bitWidth, isSigned } = (type as any as Int);
            switch (bitWidth) {
                case  8: return isSigned ? Type.Int8  : Type.Uint8 ;
                case 16: return isSigned ? Type.Int16 : Type.Uint16;
                case 32: return isSigned ? Type.Int32 : Type.Uint32;
                case 64: return isSigned ? Type.Int64 : Type.Uint64;
            }
            return Type.Int;
        case Type.Float:
            switch((type as any as Float).precision) {
                case Precision.HALF: return Type.Float16;
                case Precision.SINGLE: return Type.Float32;
                case Precision.DOUBLE: return Type.Float64;
            }
            return Type.Float;
        case Type.Binary: return Type.Binary;
        case Type.Utf8: return Type.Utf8;
        case Type.Bool: return Type.Bool;
        case Type.Decimal: return Type.Decimal;
        case Type.Time:
            switch ((type as any as Time).unit) {
                case TimeUnit.SECOND: return Type.TimeSecond;
                case TimeUnit.MILLISECOND: return Type.TimeMillisecond;
                case TimeUnit.MICROSECOND: return Type.TimeMicrosecond;
                case TimeUnit.NANOSECOND: return Type.TimeNanosecond;
            }
            return Type.Time;
        case Type.Timestamp:
            switch ((type as any as Timestamp).unit) {
                case TimeUnit.SECOND: return Type.TimestampSecond;
                case TimeUnit.MILLISECOND: return Type.TimestampMillisecond;
                case TimeUnit.MICROSECOND: return Type.TimestampMicrosecond;
                case TimeUnit.NANOSECOND: return Type.TimestampNanosecond;
            }
            return Type.Timestamp;
        case Type.Date:
            switch ((type as any as Date_).unit) {
                case DateUnit.DAY: return Type.DateDay;
                case DateUnit.MILLISECOND: return Type.DateMillisecond;
            }
            return Type.Date;
        case Type.Interval:
            switch ((type as any as Interval).unit) {
                case IntervalUnit.DAY_TIME: return Type.IntervalDayTime;
                case IntervalUnit.YEAR_MONTH: return Type.IntervalYearMonth;
            }
            return Type.Interval;
        case Type.List: return Type.List;
        case Type.Struct: return Type.Struct;
        case Type.Union:
            switch ((type as any as Union).mode) {
                case UnionMode.Dense: return Type.DenseUnion;
                case UnionMode.Sparse: return Type.SparseUnion;
            }
            return Type.Union;
        case Type.FixedSizeBinary: return Type.FixedSizeBinary;
        case Type.FixedSizeList: return Type.FixedSizeList;
        case Type.Map: return Type.Map;
        case Type.Dictionary: return Type.Dictionary;
    }
    throw new Error(`Unrecognized type '${Type[type.TType]}'`);
}

function getVisitFn<T extends DataType, K extends VisitorKind, V extends Visitor<K, any>>(visitor: V, node: VisitorNode<T>[K], throwIfNotFound = true) {
    let fn: any;
    let dtype = (typeof node === 'number')
        ? node as T['TType']
        : node instanceof DataType ? inferDType(node)
        : node instanceof Data ? inferDType(node.type)
        : node instanceof Vector ? inferDType(node.type)
        : Type.NONE;
    switch(dtype) {
        case Type.Null:                 fn = visitor.visitNull; break;
        case Type.Bool:                 fn = visitor.visitBool; break;
        case Type.Int:                  fn = visitor.visitInt; break;
        case Type.Int8:                 fn = visitor.visitInt8 || visitor.visitInt; break;
        case Type.Int16:                fn = visitor.visitInt16 || visitor.visitInt; break;
        case Type.Int32:                fn = visitor.visitInt32 || visitor.visitInt; break;
        case Type.Int64:                fn = visitor.visitInt64 || visitor.visitInt; break;
        case Type.Uint8:                fn = visitor.visitUint8 || visitor.visitInt; break;
        case Type.Uint16:               fn = visitor.visitUint16 || visitor.visitInt; break;
        case Type.Uint32:               fn = visitor.visitUint32 || visitor.visitInt; break;
        case Type.Uint64:               fn = visitor.visitUint64 || visitor.visitInt; break;
        case Type.Float:                fn = visitor.visitFloat; break;
        case Type.Float16:              fn = visitor.visitFloat16 || visitor.visitFloat; break;
        case Type.Float32:              fn = visitor.visitFloat32 || visitor.visitFloat; break;
        case Type.Float64:              fn = visitor.visitFloat64 || visitor.visitFloat; break;
        case Type.Utf8:                 fn = visitor.visitUtf8; break;
        case Type.Binary:               fn = visitor.visitBinary; break;
        case Type.FixedSizeBinary:      fn = visitor.visitFixedSizeBinary; break;
        case Type.Date:                 fn = visitor.visitDate; break;
        case Type.DateDay:              fn = visitor.visitDateDay || visitor.visitDate; break;
        case Type.DateMillisecond:      fn = visitor.visitDateMillisecond || visitor.visitDate; break;
        case Type.Timestamp:            fn = visitor.visitTimestamp; break;
        case Type.TimestampSecond:      fn = visitor.visitTimestampSecond || visitor.visitTimestamp; break;
        case Type.TimestampMillisecond: fn = visitor.visitTimestampMillisecond || visitor.visitTimestamp; break;
        case Type.TimestampMicrosecond: fn = visitor.visitTimestampMicrosecond || visitor.visitTimestamp; break;
        case Type.TimestampNanosecond:  fn = visitor.visitTimestampNanosecond || visitor.visitTimestamp; break;
        case Type.Time:                 fn = visitor.visitTime; break;
        case Type.TimeSecond:           fn = visitor.visitTimeSecond || visitor.visitTime; break;
        case Type.TimeMillisecond:      fn = visitor.visitTimeMillisecond || visitor.visitTime; break;
        case Type.TimeMicrosecond:      fn = visitor.visitTimeMicrosecond || visitor.visitTime; break;
        case Type.TimeNanosecond:       fn = visitor.visitTimeNanosecond || visitor.visitTime; break;
        case Type.Decimal:              fn = visitor.visitDecimal; break;
        case Type.List:                 fn = visitor.visitList; break;
        case Type.Struct:               fn = visitor.visitStruct; break;
        case Type.Union:                fn = visitor.visitUnion; break;
        case Type.DenseUnion:           fn = visitor.visitDenseUnion || visitor.visitUnion; break;
        case Type.SparseUnion:          fn = visitor.visitSparseUnion || visitor.visitUnion; break;
        case Type.Dictionary:           fn = visitor.visitDictionary; break;
        case Type.Interval:             fn = visitor.visitInterval; break;
        case Type.IntervalDayTime:      fn = visitor.visitIntervalDayTime || visitor.visitInterval; break;
        case Type.IntervalYearMonth:    fn = visitor.visitIntervalYearMonth || visitor.visitInterval; break;
        case Type.FixedSizeList:        fn = visitor.visitFixedSizeList; break;
        case Type.Map:                  fn = visitor.visitMap; break;
    }
    if (typeof fn === 'function') return fn;
    if (!throwIfNotFound) return () => null;
    throw new Error(`Unrecognized type '${Type[dtype]}'`);
}
