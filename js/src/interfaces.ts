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
import { Type } from './enum';
import * as type from './type';
import * as vecs from './vector';
import { DataType } from './type';

export interface ArrayBufferViewConstructor<T extends ArrayBufferView> {
    readonly prototype: T;
    new(length: number): T;
    new(arrayOrArrayBuffer: ArrayLike<number> | ArrayBufferLike): T;
    new(buffer: ArrayBufferLike, byteOffset: number, length?: number): T;
    /**
      * The size in bytes of each element in the array.
      */
    readonly BYTES_PER_ELEMENT: number;
    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): T;
    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): T;
}

// export type FunctionArgs<F> = F extends (...args: infer T) => any ? T : never;
// export type ConstructorArgs<TCtor> = TCtor extends new (_: any, ...args: infer TArgs) => any ? TArgs : never;

/**
 * Obtain the constructor function args of an instance type
 */
export type ConstructorArgs<
    T,
    TArgs extends any[] = any[],
    TCtor extends new (_: any, ...args: TArgs) => T =
                  new (_: any, ...args: TArgs) => T
> = TCtor extends new (_: any, ...args: infer TArgs) => T ? TArgs : never;

export type VectorCtorArgs<
    T extends Vector<R>,
    R extends DataType = any,
    TArgs extends any[] = any[],
    TCtor extends new (data: Data<R>, ...args: TArgs) => T =
                  new (data: Data<R>, ...args: TArgs) => T
> = TCtor extends new (data: Data<R>, ...args: infer TArgs) => T ? TArgs : never;

/**
 * Obtain the constructor function of an instance type
 */
export type ConstructorType<
    T,
    TCtor extends new (...args: any[]) => T =
                  new (...args: any[]) => T
> = TCtor extends new (...args: any[]) => infer T ? TCtor : never;

export type VectorCtorType<
    T extends Vector<R>,
    R extends DataType = any,
    TCtor extends new (data: Data<R>, ...args: VectorCtorArgs<T, R>) => T =
                  new (data: Data<R>, ...args: VectorCtorArgs<T, R>) => T
> = TCtor extends new (data: Data<R>, ...args: VectorCtorArgs<T, R>) => infer T ? TCtor : never;

export type Vector<T extends Type | DataType = any> =
    T extends Type          ? TypeToVector<T>     :
    T extends DataType      ? DataTypeToVector<T> :
                              never
    ;

export type VectorCtor<T extends Type | DataType | Vector> =
    T extends Vector        ? VectorCtorType<T>                  :
    T extends Type          ? VectorCtorType<Vector<T>>          :
    T extends DataType      ? VectorCtorType<Vector<T['TType']>> :
                              VectorCtorType<vecs.Vector>
    ;

export type DataTypeCtor<T extends Type | DataType | Vector = any> =
    T extends DataType      ? ConstructorType<T>                 :
    T extends Vector        ? ConstructorType<T['type']>         :
    T extends Type          ? ConstructorType<TypeToDataType<T>> :
                              never
    ;

type TypeToVector<T extends Type> =
    T extends Type.Null                 ? vecs.NullVector                 :
    T extends Type.Bool                 ? vecs.BoolVector                 :
    T extends Type.Int                  ? vecs.IntVector                  :
    T extends Type.Int8                 ? vecs.Int8Vector                 :
    T extends Type.Int16                ? vecs.Int16Vector                :
    T extends Type.Int32                ? vecs.Int32Vector                :
    T extends Type.Int64                ? vecs.Int64Vector                :
    T extends Type.Uint8                ? vecs.Uint8Vector                :
    T extends Type.Uint16               ? vecs.Uint16Vector               :
    T extends Type.Uint32               ? vecs.Uint32Vector               :
    T extends Type.Uint64               ? vecs.Uint64Vector               :
    T extends Type.Float                ? vecs.FloatVector                :
    T extends Type.Float16              ? vecs.Float16Vector              :
    T extends Type.Float32              ? vecs.Float32Vector              :
    T extends Type.Float64              ? vecs.Float64Vector              :
    T extends Type.Utf8                 ? vecs.Utf8Vector                 :
    T extends Type.Binary               ? vecs.BinaryVector               :
    T extends Type.FixedSizeBinary      ? vecs.FixedSizeBinaryVector      :
    T extends Type.Date                 ? vecs.DateVector                 :
    T extends Type.DateDay              ? vecs.DateDayVector              :
    T extends Type.DateMillisecond      ? vecs.DateMillisecondVector      :
    T extends Type.Timestamp            ? vecs.TimestampVector            :
    T extends Type.TimestampSecond      ? vecs.TimestampSecondVector      :
    T extends Type.TimestampMillisecond ? vecs.TimestampMillisecondVector :
    T extends Type.TimestampMicrosecond ? vecs.TimestampMicrosecondVector :
    T extends Type.TimestampNanosecond  ? vecs.TimestampNanosecondVector  :
    T extends Type.Time                 ? vecs.TimeVector                 :
    T extends Type.TimeSecond           ? vecs.TimeSecondVector           :
    T extends Type.TimeMillisecond      ? vecs.TimeMillisecondVector      :
    T extends Type.TimeMicrosecond      ? vecs.TimeMicrosecondVector      :
    T extends Type.TimeNanosecond       ? vecs.TimeNanosecondVector       :
    T extends Type.Decimal              ? vecs.DecimalVector              :
    T extends Type.Union                ? vecs.UnionVector                :
    T extends Type.DenseUnion           ? vecs.DenseUnionVector           :
    T extends Type.SparseUnion          ? vecs.SparseUnionVector          :
    T extends Type.Interval             ? vecs.IntervalVector             :
    T extends Type.IntervalDayTime      ? vecs.IntervalDayTimeVector      :
    T extends Type.IntervalYearMonth    ? vecs.IntervalYearMonthVector    :
    T extends Type.Map                  ? vecs.MapVector                  :
    T extends Type.List                 ? vecs.ListVector                 :
    T extends Type.Struct               ? vecs.StructVector               :
    T extends Type.Dictionary           ? vecs.DictionaryVector           :
    T extends Type.FixedSizeList        ? vecs.FixedSizeListVector        :
                                          vecs.Vector
    ;

type DataTypeToVector<T extends DataType = any> =
    T extends type.Null                 ? vecs.NullVector                          :
    T extends type.Bool                 ? vecs.BoolVector                          :
    T extends type.Int                  ? vecs.IntVector                           :
    T extends type.Int8                 ? vecs.Int8Vector                          :
    T extends type.Int16                ? vecs.Int16Vector                         :
    T extends type.Int32                ? vecs.Int32Vector                         :
    T extends type.Int64                ? vecs.Int64Vector                         :
    T extends type.Uint8                ? vecs.Uint8Vector                         :
    T extends type.Uint16               ? vecs.Uint16Vector                        :
    T extends type.Uint32               ? vecs.Uint32Vector                        :
    T extends type.Uint64               ? vecs.Uint64Vector                        :
    T extends type.Float                ? vecs.FloatVector                         :
    T extends type.Float16              ? vecs.Float16Vector                       :
    T extends type.Float32              ? vecs.Float32Vector                       :
    T extends type.Float64              ? vecs.Float64Vector                       :
    T extends type.Utf8                 ? vecs.Utf8Vector                          :
    T extends type.Binary               ? vecs.BinaryVector                        :
    T extends type.FixedSizeBinary      ? vecs.FixedSizeBinaryVector               :
    T extends type.Date_                ? vecs.DateVector                          :
    T extends type.DateDay              ? vecs.DateDayVector                       :
    T extends type.DateMillisecond      ? vecs.DateMillisecondVector               :
    T extends type.Timestamp            ? vecs.TimestampVector                     :
    T extends type.TimestampSecond      ? vecs.TimestampSecondVector               :
    T extends type.TimestampMillisecond ? vecs.TimestampMillisecondVector          :
    T extends type.TimestampMicrosecond ? vecs.TimestampMicrosecondVector          :
    T extends type.TimestampNanosecond  ? vecs.TimestampNanosecondVector           :
    T extends type.Time                 ? vecs.TimeVector                          :
    T extends type.TimeSecond           ? vecs.TimeSecondVector                    :
    T extends type.TimeMillisecond      ? vecs.TimeMillisecondVector               :
    T extends type.TimeMicrosecond      ? vecs.TimeMicrosecondVector               :
    T extends type.TimeNanosecond       ? vecs.TimeNanosecondVector                :
    T extends type.Decimal              ? vecs.DecimalVector                       :
    T extends type.Union                ? vecs.UnionVector                         :
    T extends type.DenseUnion           ? vecs.DenseUnionVector                    :
    T extends type.SparseUnion          ? vecs.SparseUnionVector                   :
    T extends type.Interval             ? vecs.IntervalVector                      :
    T extends type.IntervalDayTime      ? vecs.IntervalDayTimeVector               :
    T extends type.IntervalYearMonth    ? vecs.IntervalYearMonthVector             :
    T extends type.Map_                 ? vecs.MapVector<T['dataTypes']>           :
    T extends type.List                 ? vecs.ListVector<T['valueType']>          :
    T extends type.Struct               ? vecs.StructVector<T['dataTypes']>        :
    T extends type.Dictionary           ? vecs.DictionaryVector<T['valueType']>    :
    T extends type.FixedSizeList        ? vecs.FixedSizeListVector<T['valueType']> :
                                          vecs.Vector<T>
    ;

type TypeToDataType<T extends Type> =
      T extends Type.Null                 ? type.Null
    : T extends Type.Bool                 ? type.Bool
    : T extends Type.Int                  ? type.Int
    : T extends Type.Int8                 ? type.Int8
    : T extends Type.Int16                ? type.Int16
    : T extends Type.Int32                ? type.Int32
    : T extends Type.Int64                ? type.Int64
    : T extends Type.Uint8                ? type.Uint8
    : T extends Type.Uint16               ? type.Uint16
    : T extends Type.Uint32               ? type.Uint32
    : T extends Type.Uint64               ? type.Uint64
    : T extends Type.Float                ? type.Float
    : T extends Type.Float16              ? type.Float16
    : T extends Type.Float32              ? type.Float32
    : T extends Type.Float64              ? type.Float64
    : T extends Type.Utf8                 ? type.Utf8
    : T extends Type.Binary               ? type.Binary
    : T extends Type.FixedSizeBinary      ? type.FixedSizeBinary
    : T extends Type.Date                 ? type.Date_
    : T extends Type.DateDay              ? type.DateDay
    : T extends Type.DateMillisecond      ? type.DateMillisecond
    : T extends Type.Timestamp            ? type.Timestamp
    : T extends Type.TimestampSecond      ? type.TimestampSecond
    : T extends Type.TimestampMillisecond ? type.TimestampMillisecond
    : T extends Type.TimestampMicrosecond ? type.TimestampMicrosecond
    : T extends Type.TimestampNanosecond  ? type.TimestampNanosecond
    : T extends Type.Time                 ? type.Time
    : T extends Type.TimeSecond           ? type.TimeSecond
    : T extends Type.TimeMillisecond      ? type.TimeMillisecond
    : T extends Type.TimeMicrosecond      ? type.TimeMicrosecond
    : T extends Type.TimeNanosecond       ? type.TimeNanosecond
    : T extends Type.Decimal              ? type.Decimal
    : T extends Type.Union                ? type.Union
    : T extends Type.DenseUnion           ? type.DenseUnion
    : T extends Type.SparseUnion          ? type.SparseUnion
    : T extends Type.Interval             ? type.Interval
    : T extends Type.IntervalDayTime      ? type.IntervalDayTime
    : T extends Type.IntervalYearMonth    ? type.IntervalYearMonth
    : T extends Type.Map                  ? type.Map_
    : T extends Type.List                 ? type.List
    : T extends Type.Struct               ? type.Struct
    : T extends Type.Dictionary           ? type.Dictionary
    : T extends Type.FixedSizeList        ? type.FixedSizeList
                                          : DataType;

/**
 * Obtain the constructor function type of an instance type
 */
// type ConstructorType<T extends Vector, R = new (...args: any[]) => T> = R;
// type ConstructorArgs<F> = F extends new (...args: infer T) => any ? T : never;

/*

export type TypeToDataType<T extends Type = any> =
      T extends Type.Null                 ? NullVector
    : T extends Type.Bool                 ? BoolVector
    : T extends Type.Int                  ? IntVector
    : T extends Type.Int8                 ? Int8Vector
    : T extends Type.Int16                ? Int16Vector
    : T extends Type.Int32                ? Int32Vector
    : T extends Type.Int64                ? Int64Vector
    : T extends Type.Uint8                ? Uint8Vector
    : T extends Type.Uint16               ? Uint16Vector
    : T extends Type.Uint32               ? Uint32Vector
    : T extends Type.Uint64               ? Uint64Vector
    : T extends Type.Float                ? FloatVector
    : T extends Type.Float16              ? Float16Vector
    : T extends Type.Float32              ? Float32Vector
    : T extends Type.Float64              ? Float64Vector
    : T extends Type.Utf8                 ? Utf8Vector
    : T extends Type.Binary               ? BinaryVector
    : T extends Type.FixedSizeBinary      ? FixedSizeBinaryVector
    : T extends Type.Date                 ? DateVector
    : T extends Type.DateDay              ? DateDayVector
    : T extends Type.DateMillisecond      ? DateMillisecondVector
    : T extends Type.Timestamp            ? TimestampVector
    : T extends Type.TimestampSecond      ? TimestampSecondVector
    : T extends Type.TimestampMillisecond ? TimestampMillisecondVector
    : T extends Type.TimestampMicrosecond ? TimestampMicrosecondVector
    : T extends Type.TimestampNanosecond  ? TimestampNanosecondVector
    : T extends Type.Time                 ? TimeVector
    : T extends Type.TimeSecond           ? TimeSecondVector
    : T extends Type.TimeMillisecond      ? TimeMillisecondVector
    : T extends Type.TimeMicrosecond      ? TimeMicrosecondVector
    : T extends Type.TimeNanosecond       ? TimeNanosecondVector
    : T extends Type.Decimal              ? DecimalVector
    : T extends Type.Union                ? UnionVector
    : T extends Type.DenseUnion           ? DenseUnionVector
    : T extends Type.SparseUnion          ? SparseUnionVector
    : T extends Type.Interval             ? IntervalVector
    : T extends Type.IntervalDayTime      ? IntervalDayTimeVector
    : T extends Type.IntervalYearMonth    ? IntervalYearMonthVector
    : T extends Type.Map                  ? MapVector
    : T extends Type.List                 ? ListVector
    : T extends Type.Struct               ? StructVector
    : T extends Type.Dictionary           ? DictionaryVector
    : T extends Type.FixedSizeList        ? FixedSizeListVector
                                          : Vector;

// export type TypeToDataType<T extends DataType = any> = {
//     [Type.NONE]: any;
//     [Type.Null]: Null;
//     [Type.Bool]: Bool;
//     [Type.Int]: Int;
//     [Type.Int8]: Int8;
//     [Type.Int16]: Int16;
//     [Type.Int32]: Int32;
//     [Type.Int64]: Int64;
//     [Type.Uint8]: Uint8;
//     [Type.Uint16]: Uint16;
//     [Type.Uint32]: Uint32;
//     [Type.Uint64]: Uint64;
//     [Type.Float]: Float;
//     [Type.Float16]: Float16;
//     [Type.Float32]: Float32;
//     [Type.Float64]: Float64;
//     [Type.Utf8]: Utf8;
//     [Type.Binary]: Binary;
//     [Type.FixedSizeBinary]: FixedSizeBinary;
//     [Type.Date]: Date_;
//     [Type.DateDay]: DateDay;
//     [Type.DateMillisecond]: DateMillisecond;
//     [Type.Timestamp]: Timestamp;
//     [Type.TimestampSecond]: TimestampSecond;
//     [Type.TimestampMillisecond]: TimestampMillisecond;
//     [Type.TimestampMicrosecond]: TimestampMicrosecond;
//     [Type.TimestampNanosecond]: TimestampNanosecond;
//     [Type.Time]: Time;
//     [Type.TimeSecond]: TimeSecond;
//     [Type.TimeMillisecond]: TimeMillisecond;
//     [Type.TimeMicrosecond]: TimeMicrosecond;
//     [Type.TimeNanosecond]: TimeNanosecond;
//     [Type.Decimal]: Decimal;
//     [Type.Union]: Union;
//     [Type.DenseUnion]: DenseUnion;
//     [Type.SparseUnion]: SparseUnion;
//     [Type.Interval]: Interval;
//     [Type.IntervalDayTime]: IntervalDayTime;
//     [Type.IntervalYearMonth]: IntervalYearMonth;
//     [Type.Map]: Map_<T extends Map_ ? T['dataTypes'] : any>;
//     [Type.List]: List<T extends List ? T['valueType'] : any>;
//     [Type.Struct]: Struct<T extends Struct ? T['dataTypes'] : any>;
//     [Type.Dictionary]: Dictionary<T extends Dictionary ? T['valueType'] : any>;
//     [Type.FixedSizeList]: FixedSizeList<T extends FixedSizeList ? T['valueType'] : any>;
// }


// export type DataTypeToVectorConstructor<T extends DataType = any> =
//       T extends Null                 ? vectorCtors.NullVector
//     : T extends Bool                 ? vectorCtors.BoolVector
//     : T extends Int                  ? vectorCtors.IntVector
//     : T extends Int8                 ? vectorCtors.Int8Vector
//     : T extends Int16                ? vectorCtors.Int16Vector
//     : T extends Int32                ? vectorCtors.Int32Vector
//     : T extends Int64                ? vectorCtors.Int64Vector
//     : T extends Uint8                ? vectorCtors.Uint8Vector
//     : T extends Uint16               ? vectorCtors.Uint16Vector
//     : T extends Uint32               ? vectorCtors.Uint32Vector
//     : T extends Uint64               ? vectorCtors.Uint64Vector
//     : T extends Float                ? vectorCtors.FloatVector
//     : T extends Float16              ? vectorCtors.Float16Vector
//     : T extends Float32              ? vectorCtors.Float32Vector
//     : T extends Float64              ? vectorCtors.Float64Vector
//     : T extends Utf8                 ? vectorCtors.Utf8Vector
//     : T extends Binary               ? vectorCtors.BinaryVector
//     : T extends FixedSizeBinary      ? vectorCtors.FixedSizeBinaryVector
//     : T extends Date_                ? vectorCtors.DateVector
//     : T extends DateDay              ? vectorCtors.DateDayVector
//     : T extends DateMillisecond      ? vectorCtors.DateMillisecondVector
//     : T extends Timestamp            ? vectorCtors.TimestampVector
//     : T extends TimestampSecond      ? vectorCtors.TimestampSecondVector
//     : T extends TimestampMillisecond ? vectorCtors.TimestampMillisecondVector
//     : T extends TimestampMicrosecond ? vectorCtors.TimestampMicrosecondVector
//     : T extends TimestampNanosecond  ? vectorCtors.TimestampNanosecondVector
//     : T extends Time                 ? vectorCtors.TimeVector
//     : T extends TimeSecond           ? vectorCtors.TimeSecondVector
//     : T extends TimeMillisecond      ? vectorCtors.TimeMillisecondVector
//     : T extends TimeMicrosecond      ? vectorCtors.TimeMicrosecondVector
//     : T extends TimeNanosecond       ? vectorCtors.TimeNanosecondVector
//     : T extends Decimal              ? vectorCtors.DecimalVector
//     : T extends Union                ? vectorCtors.UnionVector
//     : T extends DenseUnion           ? vectorCtors.DenseUnionVector
//     : T extends SparseUnion          ? vectorCtors.SparseUnionVector
//     : T extends Interval             ? vectorCtors.IntervalVector
//     : T extends IntervalDayTime      ? vectorCtors.IntervalDayTimeVector
//     : T extends IntervalYearMonth    ? vectorCtors.IntervalYearMonthVector
//     : T extends Map_                 ? vectorCtors.MapVector<T['dataTypes']>
//     : T extends List                 ? vectorCtors.ListVector<T['valueType']>
//     : T extends Struct               ? vectorCtors.StructVector<T['dataTypes']>
//     : T extends Dictionary           ? vectorCtors.DictionaryVector<T['valueType']>
//     : T extends FixedSizeList        ? vectorCtors.FixedSizeListVector<T['valueType']>
//                                      : vectorCtors.Vector;

// export type DataTypeToVectorConstructor<T extends DataType = any> =
//       T extends Null                 ? ConstructorType<NullVector>
//     : T extends Bool                 ? ConstructorType<BoolVector>
//     : T extends Int                  ? ConstructorType<IntVector>
//     : T extends Int8                 ? ConstructorType<Int8Vector>
//     : T extends Int16                ? ConstructorType<Int16Vector>
//     : T extends Int32                ? ConstructorType<Int32Vector>
//     : T extends Int64                ? ConstructorType<Int64Vector>
//     : T extends Uint8                ? ConstructorType<Uint8Vector>
//     : T extends Uint16               ? ConstructorType<Uint16Vector>
//     : T extends Uint32               ? ConstructorType<Uint32Vector>
//     : T extends Uint64               ? ConstructorType<Uint64Vector>
//     : T extends Float                ? ConstructorType<FloatVector>
//     : T extends Float16              ? ConstructorType<Float16Vector>
//     : T extends Float32              ? ConstructorType<Float32Vector>
//     : T extends Float64              ? ConstructorType<Float64Vector>
//     : T extends Utf8                 ? ConstructorType<Utf8Vector>
//     : T extends Binary               ? ConstructorType<BinaryVector>
//     : T extends FixedSizeBinary      ? ConstructorType<FixedSizeBinaryVector>
//     : T extends Date_                ? ConstructorType<DateVector>
//     : T extends DateDay              ? ConstructorType<DateDayVector>
//     : T extends DateMillisecond      ? ConstructorType<DateMillisecondVector>
//     : T extends Timestamp            ? ConstructorType<TimestampVector>
//     : T extends TimestampSecond      ? ConstructorType<TimestampSecondVector>
//     : T extends TimestampMillisecond ? ConstructorType<TimestampMillisecondVector>
//     : T extends TimestampMicrosecond ? ConstructorType<TimestampMicrosecondVector>
//     : T extends TimestampNanosecond  ? ConstructorType<TimestampNanosecondVector>
//     : T extends Time                 ? ConstructorType<TimeVector>
//     : T extends TimeSecond           ? ConstructorType<TimeSecondVector>
//     : T extends TimeMillisecond      ? ConstructorType<TimeMillisecondVector>
//     : T extends TimeMicrosecond      ? ConstructorType<TimeMicrosecondVector>
//     : T extends TimeNanosecond       ? ConstructorType<TimeNanosecondVector>
//     : T extends Decimal              ? ConstructorType<DecimalVector>
//     : T extends Union                ? ConstructorType<UnionVector>
//     : T extends DenseUnion           ? ConstructorType<DenseUnionVector>
//     : T extends SparseUnion          ? ConstructorType<SparseUnionVector>
//     : T extends Interval             ? ConstructorType<IntervalVector>
//     : T extends IntervalDayTime      ? ConstructorType<IntervalDayTimeVector>
//     : T extends IntervalYearMonth    ? ConstructorType<IntervalYearMonthVector>
//     : T extends Map_                 ? ConstructorType<MapVector<T['dataTypes']>>
//     : T extends List                 ? ConstructorType<ListVector<T['valueType']>>
//     : T extends Struct               ? ConstructorType<StructVector<T['dataTypes']>>
//     : T extends Dictionary           ? ConstructorType<DictionaryVector<T['valueType']>>
//     : T extends FixedSizeList        ? ConstructorType<FixedSizeListVector<T['valueType']>>
//                                      : ConstructorType<Vector>
//                                      ;

// export type TypeToVectorConstructor = {
//     [Type.NONE]                 : ConstructorType<Vector>;
//     [Type.Null]                 : ConstructorType<NullVector>;
//     [Type.Bool]                 : ConstructorType<BoolVector>;
//     [Type.Int]                  : ConstructorType<IntVector>;
//     [Type.Int8]                 : ConstructorType<Int8Vector>;
//     [Type.Int16]                : ConstructorType<Int16Vector>;
//     [Type.Int32]                : ConstructorType<Int32Vector>;
//     [Type.Int64]                : ConstructorType<Int64Vector>;
//     [Type.Uint8]                : ConstructorType<Uint8Vector>;
//     [Type.Uint16]               : ConstructorType<Uint16Vector>;
//     [Type.Uint32]               : ConstructorType<Uint32Vector>;
//     [Type.Uint64]               : ConstructorType<Uint64Vector>;
//     [Type.Float]                : ConstructorType<FloatVector>;
//     [Type.Float16]              : ConstructorType<Float16Vector>;
//     [Type.Float32]              : ConstructorType<Float32Vector>;
//     [Type.Float64]              : ConstructorType<Float64Vector>;
//     [Type.Utf8]                 : ConstructorType<Utf8Vector>;
//     [Type.Binary]               : ConstructorType<BinaryVector>;
//     [Type.FixedSizeBinary]      : ConstructorType<FixedSizeBinaryVector>;
//     [Type.Date]                 : ConstructorType<DateVector>;
//     [Type.DateDay]              : ConstructorType<DateDayVector>;
//     [Type.DateMillisecond]      : ConstructorType<DateMillisecondVector>;
//     [Type.Timestamp]            : ConstructorType<TimestampVector>;
//     [Type.TimestampSecond]      : ConstructorType<TimestampSecondVector>;
//     [Type.TimestampMillisecond] : ConstructorType<TimestampMillisecondVector>;
//     [Type.TimestampMicrosecond] : ConstructorType<TimestampMicrosecondVector>;
//     [Type.TimestampNanosecond]  : ConstructorType<TimestampNanosecondVector>;
//     [Type.Time]                 : ConstructorType<TimeVector>;
//     [Type.TimeSecond]           : ConstructorType<TimeSecondVector>;
//     [Type.TimeMillisecond]      : ConstructorType<TimeMillisecondVector>;
//     [Type.TimeMicrosecond]      : ConstructorType<TimeMicrosecondVector>;
//     [Type.TimeNanosecond]       : ConstructorType<TimeNanosecondVector>;
//     [Type.Decimal]              : ConstructorType<DecimalVector>;
//     [Type.List]                 : ConstructorType<ListVector>;
//     [Type.Struct]               : ConstructorType<StructVector>;
//     [Type.Union]                : ConstructorType<UnionVector>;
//     [Type.DenseUnion]           : ConstructorType<DenseUnionVector>;
//     [Type.SparseUnion]          : ConstructorType<SparseUnionVector>;
//     [Type.Dictionary]           : ConstructorType<DictionaryVector>;
//     [Type.Interval]             : ConstructorType<IntervalVector>;
//     [Type.IntervalDayTime]      : ConstructorType<IntervalDayTimeVector>;
//     [Type.IntervalYearMonth]    : ConstructorType<IntervalYearMonthVector>;
//     [Type.FixedSizeList]        : ConstructorType<FixedSizeListVector>;
//     [Type.Map]                  : ConstructorType<MapVector>;
// };

export type TypeToDataTypeConstructor = {
    [Type.NONE]                 : (new                           (...args: any[]) => any);
    [Type.Null]                 : (new                           (...args: any[]) => Null);
    [Type.Bool]                 : (new                           (...args: any[]) => Bool);
    [Type.Int]                  : (new                           (...args: any[]) => Int);
    [Type.Int8]                 : (new                           (...args: any[]) => Int8);
    [Type.Int16]                : (new                           (...args: any[]) => Int16);
    [Type.Int32]                : (new                           (...args: any[]) => Int32);
    [Type.Int64]                : (new                           (...args: any[]) => Int64);
    [Type.Uint8]                : (new                           (...args: any[]) => Uint8);
    [Type.Uint16]               : (new                           (...args: any[]) => Uint16);
    [Type.Uint32]               : (new                           (...args: any[]) => Uint32);
    [Type.Uint64]               : (new                           (...args: any[]) => Uint64);
    [Type.Float]                : (new                           (...args: any[]) => Float);
    [Type.Float16]              : (new                           (...args: any[]) => Float16);
    [Type.Float32]              : (new                           (...args: any[]) => Float32);
    [Type.Float64]              : (new                           (...args: any[]) => Float64);
    [Type.Utf8]                 : (new                           (...args: any[]) => Utf8);
    [Type.Binary]               : (new                           (...args: any[]) => Binary);
    [Type.FixedSizeBinary]      : (new                           (...args: any[]) => FixedSizeBinary);
    [Type.Date]                 : (new                           (...args: any[]) => Date_);
    [Type.DateDay]              : (new                           (...args: any[]) => DateDay);
    [Type.DateMillisecond]      : (new                           (...args: any[]) => DateMillisecond);
    [Type.Timestamp]            : (new                           (...args: any[]) => Timestamp);
    [Type.TimestampSecond]      : (new                           (...args: any[]) => TimestampSecond);
    [Type.TimestampMillisecond] : (new                           (...args: any[]) => TimestampMillisecond);
    [Type.TimestampMicrosecond] : (new                           (...args: any[]) => TimestampMicrosecond);
    [Type.TimestampNanosecond]  : (new                           (...args: any[]) => TimestampNanosecond);
    [Type.Time]                 : (new                           (...args: any[]) => Time);
    [Type.TimeSecond]           : (new                           (...args: any[]) => TimeSecond);
    [Type.TimeMillisecond]      : (new                           (...args: any[]) => TimeMillisecond);
    [Type.TimeMicrosecond]      : (new                           (...args: any[]) => TimeMicrosecond);
    [Type.TimeNanosecond]       : (new                           (...args: any[]) => TimeNanosecond);
    [Type.Decimal]              : (new                           (...args: any[]) => Decimal);
    [Type.List]                 : (new <T extends DataType = any>(...args: any[]) => List<T>);
    [Type.Struct]               : (new                           (...args: any[]) => Struct);
    [Type.Union]                : (new                           (...args: any[]) => Union);
    [Type.DenseUnion]           : (new                           (...args: any[]) => DenseUnion);
    [Type.SparseUnion]          : (new                           (...args: any[]) => SparseUnion);
    [Type.Dictionary]           : (new <T extends DataType = any>(...args: any[]) => Dictionary<T>);
    [Type.Interval]             : (new                           (...args: any[]) => Interval);
    [Type.IntervalDayTime]      : (new                           (...args: any[]) => IntervalDayTime);
    [Type.IntervalYearMonth]    : (new                           (...args: any[]) => IntervalYearMonth);
    [Type.FixedSizeList]        : (new <T extends DataType = any>(...args: any[]) => FixedSizeList<T>);
    [Type.Map]                  : (new                           (...args: any[]) => Map_);
};

// export type TypeToVectorConstructor = {
//     [Type.NONE]                 : vectorCtors.Vector;
//     [Type.Null]                 : vectorCtors.NullVector;
//     [Type.Bool]                 : vectorCtors.BoolVector;
//     [Type.Int]                  : vectorCtors.IntVector;
//     [Type.Int8]                 : vectorCtors.Int8Vector;
//     [Type.Int16]                : vectorCtors.Int16Vector;
//     [Type.Int32]                : vectorCtors.Int32Vector;
//     [Type.Int64]                : vectorCtors.Int64Vector;
//     [Type.Uint8]                : vectorCtors.Uint8Vector;
//     [Type.Uint16]               : vectorCtors.Uint16Vector;
//     [Type.Uint32]               : vectorCtors.Uint32Vector;
//     [Type.Uint64]               : vectorCtors.Uint64Vector;
//     [Type.Float]                : vectorCtors.FloatVector;
//     [Type.Float16]              : vectorCtors.Float16Vector;
//     [Type.Float32]              : vectorCtors.Float32Vector;
//     [Type.Float64]              : vectorCtors.Float64Vector;
//     [Type.Utf8]                 : vectorCtors.Utf8Vector;
//     [Type.Binary]               : vectorCtors.BinaryVector;
//     [Type.FixedSizeBinary]      : vectorCtors.FixedSizeBinaryVector;
//     [Type.Date]                 : vectorCtors.DateVector;
//     [Type.DateDay]              : vectorCtors.DateDayVector;
//     [Type.DateMillisecond]      : vectorCtors.DateMillisecondVector;
//     [Type.Timestamp]            : vectorCtors.TimestampVector;
//     [Type.TimestampSecond]      : vectorCtors.TimestampSecondVector;
//     [Type.TimestampMillisecond] : vectorCtors.TimestampMillisecondVector;
//     [Type.TimestampMicrosecond] : vectorCtors.TimestampMicrosecondVector;
//     [Type.TimestampNanosecond]  : vectorCtors.TimestampNanosecondVector;
//     [Type.Time]                 : vectorCtors.TimeVector;
//     [Type.TimeSecond]           : vectorCtors.TimeSecondVector;
//     [Type.TimeMillisecond]      : vectorCtors.TimeMillisecondVector;
//     [Type.TimeMicrosecond]      : vectorCtors.TimeMicrosecondVector;
//     [Type.TimeNanosecond]       : vectorCtors.TimeNanosecondVector;
//     [Type.Decimal]              : vectorCtors.DecimalVector;
//     [Type.List]                 : vectorCtors.ListVector;
//     [Type.Struct]               : vectorCtors.StructVector;
//     [Type.Union]                : vectorCtors.UnionVector;
//     [Type.DenseUnion]           : vectorCtors.DenseUnionVector;
//     [Type.SparseUnion]          : vectorCtors.SparseUnionVector;
//     [Type.Dictionary]           : vectorCtors.DictionaryVector;
//     [Type.Interval]             : vectorCtors.IntervalVector;
//     [Type.IntervalDayTime]      : vectorCtors.IntervalDayTimeVector;
//     [Type.IntervalYearMonth]    : vectorCtors.IntervalYearMonthVector;
//     [Type.FixedSizeList]        : vectorCtors.FixedSizeListVector;
//     [Type.Map]                  : vectorCtors.MapVector;
// };

type VectorConstructor                     = (new <T extends DataType = any>(data: Data<T>,                    ...args: any[]) => Vector<T>);
type NullVectorConstructor                 = (new                           (data: Data<Null>,                 ...args: any[]) => NullVector);
type BoolVectorConstructor                 = (new                           (data: Data<Bool>,                 ...args: any[]) => BoolVector);
type IntVectorConstructor                  = (new                           (data: Data<Int>,                  ...args: any[]) => IntVector);
type Int8VectorConstructor                 = (new                           (data: Data<Int8>,                 ...args: any[]) => Int8Vector);
type Int16VectorConstructor                = (new                           (data: Data<Int16>,                ...args: any[]) => Int16Vector);
type Int32VectorConstructor                = (new                           (data: Data<Int32>,                ...args: any[]) => Int32Vector);
type Int64VectorConstructor                = (new                           (data: Data<Int64>,                ...args: any[]) => Int64Vector);
type Uint8VectorConstructor                = (new                           (data: Data<Uint8>,                ...args: any[]) => Uint8Vector);
type Uint16VectorConstructor               = (new                           (data: Data<Uint16>,               ...args: any[]) => Uint16Vector);
type Uint32VectorConstructor               = (new                           (data: Data<Uint32>,               ...args: any[]) => Uint32Vector);
type Uint64VectorConstructor               = (new                           (data: Data<Uint64>,               ...args: any[]) => Uint64Vector);
type FloatVectorConstructor                = (new                           (data: Data<Float>,                ...args: any[]) => FloatVector);
type Float16VectorConstructor              = (new                           (data: Data<Float16>,              ...args: any[]) => Float16Vector);
type Float32VectorConstructor              = (new                           (data: Data<Float32>,              ...args: any[]) => Float32Vector);
type Float64VectorConstructor              = (new                           (data: Data<Float64>,              ...args: any[]) => Float64Vector);
type Utf8VectorConstructor                 = (new                           (data: Data<Utf8>,                 ...args: any[]) => Utf8Vector);
type BinaryVectorConstructor               = (new                           (data: Data<Binary>,               ...args: any[]) => BinaryVector);
type FixedSizeBinaryVectorConstructor      = (new                           (data: Data<FixedSizeBinary>,      ...args: any[]) => FixedSizeBinaryVector);
type DateVectorConstructor                 = (new                           (data: Data<Date_>,                ...args: any[]) => DateVector);
type DateDayVectorConstructor              = (new                           (data: Data<DateDay>,              ...args: any[]) => DateDayVector);
type DateMillisecondVectorConstructor      = (new                           (data: Data<DateMillisecond>,      ...args: any[]) => DateMillisecondVector);
type TimestampVectorConstructor            = (new                           (data: Data<Timestamp>,            ...args: any[]) => TimestampVector);
type TimestampSecondVectorConstructor      = (new                           (data: Data<TimestampSecond>,      ...args: any[]) => TimestampSecondVector);
type TimestampMillisecondVectorConstructor = (new                           (data: Data<TimestampMillisecond>, ...args: any[]) => TimestampMillisecondVector);
type TimestampMicrosecondVectorConstructor = (new                           (data: Data<TimestampMicrosecond>, ...args: any[]) => TimestampMicrosecondVector);
type TimestampNanosecondVectorConstructor  = (new                           (data: Data<TimestampNanosecond>,  ...args: any[]) => TimestampNanosecondVector);
type TimeVectorConstructor                 = (new                           (data: Data<Time>,                 ...args: any[]) => TimeVector);
type TimeSecondVectorConstructor           = (new                           (data: Data<TimeSecond>,           ...args: any[]) => TimeSecondVector);
type TimeMillisecondVectorConstructor      = (new                           (data: Data<TimeMillisecond>,      ...args: any[]) => TimeMillisecondVector);
type TimeMicrosecondVectorConstructor      = (new                           (data: Data<TimeMicrosecond>,      ...args: any[]) => TimeMicrosecondVector);
type TimeNanosecondVectorConstructor       = (new                           (data: Data<TimeNanosecond>,       ...args: any[]) => TimeNanosecondVector);
type DecimalVectorConstructor              = (new                           (data: Data<Decimal>,              ...args: any[]) => DecimalVector);
type ListVectorConstructor                 = (new <T extends DataType = any>(data: Data<List<T>>,              ...args: any[]) => ListVector<T>);
type StructVectorConstructor               = (new                           (data: Data<Struct>,               ...args: any[]) => StructVector);
type UnionVectorConstructor                = (new                           (data: Data<Union>,                ...args: any[]) => UnionVector);
type DenseUnionVectorConstructor           = (new                           (data: Data<DenseUnion>,           ...args: any[]) => DenseUnionVector);
type SparseUnionVectorConstructor          = (new                           (data: Data<SparseUnion>,          ...args: any[]) => SparseUnionVector);
type DictionaryVectorConstructor           = (new <T extends DataType = any>(dictionary: Vector<T>, indices: Vector<Int>     ) => DictionaryVector<T>);
type IntervalVectorConstructor             = (new                           (data: Data<Interval>,             ...args: any[]) => IntervalVector);
type IntervalDayTimeVectorConstructor      = (new                           (data: Data<IntervalDayTime>,      ...args: any[]) => IntervalDayTimeVector);
type IntervalYearMonthVectorConstructor    = (new                           (data: Data<IntervalYearMonth>,    ...args: any[]) => IntervalYearMonthVector);
type FixedSizeListVectorConstructor        = (new <T extends DataType = any>(data: Data<FixedSizeList<T>>,     ...args: any[]) => FixedSizeListVector<T>);
type MapVectorConstructor                  = (new                           (data: Data<Map_>,                 ...args: any[]) => MapVector);
*/
