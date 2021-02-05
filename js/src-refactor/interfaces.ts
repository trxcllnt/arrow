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
import { DataType } from './type';
import { Vector } from './vector';
import * as builders from './builder/index';
import { BuilderOptions } from './builder/index';

/** @ignore */ type FloatArray = Float32Array | Float64Array;
/** @ignore */ type IntArray = Int8Array | Int16Array | Int32Array;
/** @ignore */ type UintArray = Uint8Array | Uint16Array | Uint32Array | Uint8ClampedArray;
/** @ignore */
export type TypedArray = FloatArray | IntArray | UintArray;
/** @ignore */
export type BigIntArray = BigInt64Array | BigUint64Array;

/** @ignore */
export interface TypedArrayConstructor<T extends TypedArray> {
    readonly prototype: T;
    new(length?: number): T;
    new(array: Iterable<number>): T;
    new(buffer: ArrayBufferLike, byteOffset?: number, length?: number): T;
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
    from<U>(arrayLike: ArrayLike<U>, mapfn: (v: U, k: number) => number, thisArg?: any): T;
}

/** @ignore */
export interface BigIntArrayConstructor<T extends BigIntArray> {
    readonly prototype: T;
    new(length?: number): T;
    new(array: Iterable<bigint>): T;
    new(buffer: ArrayBufferLike, byteOffset?: number, length?: number): T;
    /**
      * The size in bytes of each element in the array.
      */
    readonly BYTES_PER_ELEMENT: number;
    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: bigint[]): T;
    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<bigint>, mapfn?: (v: bigint, k: number) => bigint, thisArg?: any): T;
    from<U>(arrayLike: ArrayLike<U>, mapfn: (v: U, k: number) => bigint, thisArg?: any): T;
}

/** @ignore */
export type VectorCtorArgs<
    T extends VectorType<R>,
    R extends DataType = any,
    TArgs extends any[] = any[],
    TCtor extends new (data: Data<R>, ...args: TArgs) => T =
                  new (data: Data<R>, ...args: TArgs) => T
> = TCtor extends new (data: Data<R>, ...args: infer TArgs) => T ? TArgs : never;

/** @ignore */
export type BuilderCtorArgs<
    T extends BuilderType<R, any>,
    R extends DataType = any,
    TArgs extends any[] = any[],
    TCtor extends new (type: R, ...args: TArgs) => T =
                  new (type: R, ...args: TArgs) => T
> = TCtor extends new (type: R, ...args: infer TArgs) => T ? TArgs : never;

/**
 * Obtain the constructor function of an instance type
 * @ignore
 */
export type ConstructorType<
    T,
    TCtor extends new (...args: any[]) => T =
                  new (...args: any[]) => T
> = TCtor extends new (...args: any[]) => T ? TCtor : never;

/** @ignore */
export type VectorCtorType<
    T extends VectorType<R>,
    R extends DataType = any,
    TCtor extends new (type: R, data?: Data<R>[], offsets?: Uint32Array) => T =
                  new (type: R, data?: Data<R>[], offsets?: Uint32Array) => T
> = TCtor extends new (type: R, data?: Data<R>[], offsets?: Uint32Array) => T ? TCtor : never;

/** @ignore */
export type BuilderCtorType<
    T extends BuilderType<R, any>,
    R extends DataType = any,
    TCtor extends new (options: BuilderOptions<R, any>) => T =
                  new (options: BuilderOptions<R, any>) => T
> = TCtor extends new (options: BuilderOptions<R, any>) => T ? TCtor : never;

/** @ignore */
export type VectorType<T extends Type | DataType = any> =
    T extends Type          ? TypeToVector<T>     :
    T extends DataType      ? DataTypeToVector<T> :
                              Vector<any>
    ;

/** @ignore */
export type BuilderType<T extends Type | DataType = any, TNull = any> =
    T extends Type          ? TypeToBuilder<T, TNull>     :
    T extends DataType      ? DataTypeToBuilder<T, TNull> :
                              builders.Builder<any, TNull>
    ;

/** @ignore */
export type VectorCtor<T extends Type | DataType | VectorType> =
    T extends VectorType    ? VectorCtorType<VectorType<T['TType']>> :
    T extends Type          ? VectorCtorType<VectorType<T>>          :
    T extends DataType      ? VectorCtorType<VectorType<T['TType']>> :
                              VectorCtorType<Vector>
    ;

/** @ignore */
export type BuilderCtor<T extends Type | DataType = any> =
    T extends Type          ? BuilderCtorType<BuilderType<T>> :
    T extends DataType      ? BuilderCtorType<BuilderType<T>> :
                              BuilderCtorType<builders.Builder>
    ;

/** @ignore */
export type DataTypeCtor<T extends Type | DataType | Vector = any> =
    T extends DataType ? ConstructorType<T>                 :
    T extends Vector   ? ConstructorType<T['type']>         :
    T extends Type     ? ConstructorType<TypeToDataType<T>> :
                         never
    ;

/** @ignore */
type TypeToVector<T extends Type> = {
    [key: number               ]: Vector ;
    [Type.Null                 ]: Vector ;
    [Type.Bool                 ]: Vector ;
    [Type.Int8                 ]: Vector ;
    [Type.Int16                ]: Vector ;
    [Type.Int32                ]: Vector ;
    [Type.Int64                ]: Vector ;
    [Type.Uint8                ]: Vector ;
    [Type.Uint16               ]: Vector ;
    [Type.Uint32               ]: Vector ;
    [Type.Uint64               ]: Vector ;
    [Type.Int                  ]: Vector ;
    [Type.Float16              ]: Vector ;
    [Type.Float32              ]: Vector ;
    [Type.Float64              ]: Vector ;
    [Type.Float                ]: Vector ;
    [Type.Utf8                 ]: Vector ;
    [Type.Binary               ]: Vector ;
    [Type.FixedSizeBinary      ]: Vector ;
    [Type.Date                 ]: Vector ;
    [Type.DateDay              ]: Vector ;
    [Type.DateMillisecond      ]: Vector ;
    [Type.Timestamp            ]: Vector ;
    [Type.TimestampSecond      ]: Vector ;
    [Type.TimestampMillisecond ]: Vector ;
    [Type.TimestampMicrosecond ]: Vector ;
    [Type.TimestampNanosecond  ]: Vector ;
    [Type.Time                 ]: Vector ;
    [Type.TimeSecond           ]: Vector ;
    [Type.TimeMillisecond      ]: Vector ;
    [Type.TimeMicrosecond      ]: Vector ;
    [Type.TimeNanosecond       ]: Vector ;
    [Type.Decimal              ]: Vector ;
    [Type.Union                ]: Vector ;
    [Type.DenseUnion           ]: Vector ;
    [Type.SparseUnion          ]: Vector ;
    [Type.Interval             ]: Vector ;
    [Type.IntervalDayTime      ]: Vector ;
    [Type.IntervalYearMonth    ]: Vector ;
    [Type.Map                  ]: Vector ;
    [Type.List                 ]: Vector ;
    [Type.Struct               ]: Vector ;
    [Type.Dictionary           ]: Vector ;
    [Type.FixedSizeList        ]: Vector ;
}[T];

/** @ignore */
type DataTypeToVector<T extends DataType = any> = {
    [key: number               ]:                                       Vector<any>       ;
    [Type.Null                 ]: T extends type.Null                 ? Vector<T> : never ;
    [Type.Bool                 ]: T extends type.Bool                 ? Vector<T> : never ;
    [Type.Int8                 ]: T extends type.Int8                 ? Vector<T> : never ;
    [Type.Int16                ]: T extends type.Int16                ? Vector<T> : never ;
    [Type.Int32                ]: T extends type.Int32                ? Vector<T> : never ;
    [Type.Int64                ]: T extends type.Int64                ? Vector<T> : never ;
    [Type.Uint8                ]: T extends type.Uint8                ? Vector<T> : never ;
    [Type.Uint16               ]: T extends type.Uint16               ? Vector<T> : never ;
    [Type.Uint32               ]: T extends type.Uint32               ? Vector<T> : never ;
    [Type.Uint64               ]: T extends type.Uint64               ? Vector<T> : never ;
    [Type.Int                  ]: T extends type.Int                  ? Vector<T> : never ;
    [Type.Float16              ]: T extends type.Float16              ? Vector<T> : never ;
    [Type.Float32              ]: T extends type.Float32              ? Vector<T> : never ;
    [Type.Float64              ]: T extends type.Float64              ? Vector<T> : never ;
    [Type.Float                ]: T extends type.Float                ? Vector<T> : never ;
    [Type.Utf8                 ]: T extends type.Utf8                 ? Vector<T> : never ;
    [Type.Binary               ]: T extends type.Binary               ? Vector<T> : never ;
    [Type.FixedSizeBinary      ]: T extends type.FixedSizeBinary      ? Vector<T> : never ;
    [Type.Date                 ]: T extends type.Date_                ? Vector<T> : never ;
    [Type.DateDay              ]: T extends type.DateDay              ? Vector<T> : never ;
    [Type.DateMillisecond      ]: T extends type.DateMillisecond      ? Vector<T> : never ;
    [Type.Timestamp            ]: T extends type.Timestamp            ? Vector<T> : never ;
    [Type.TimestampSecond      ]: T extends type.TimestampSecond      ? Vector<T> : never ;
    [Type.TimestampMillisecond ]: T extends type.TimestampMillisecond ? Vector<T> : never ;
    [Type.TimestampMicrosecond ]: T extends type.TimestampMicrosecond ? Vector<T> : never ;
    [Type.TimestampNanosecond  ]: T extends type.TimestampNanosecond  ? Vector<T> : never ;
    [Type.Time                 ]: T extends type.Time                 ? Vector<T> : never ;
    [Type.TimeSecond           ]: T extends type.TimeSecond           ? Vector<T> : never ;
    [Type.TimeMillisecond      ]: T extends type.TimeMillisecond      ? Vector<T> : never ;
    [Type.TimeMicrosecond      ]: T extends type.TimeMicrosecond      ? Vector<T> : never ;
    [Type.TimeNanosecond       ]: T extends type.TimeNanosecond       ? Vector<T> : never ;
    [Type.Decimal              ]: T extends type.Decimal              ? Vector<T> : never ;
    [Type.Union                ]: T extends type.Union                ? Vector<T> : never ;
    [Type.DenseUnion           ]: T extends type.DenseUnion           ? Vector<T> : never ;
    [Type.SparseUnion          ]: T extends type.SparseUnion          ? Vector<T> : never ;
    [Type.Interval             ]: T extends type.Interval             ? Vector<T> : never ;
    [Type.IntervalDayTime      ]: T extends type.IntervalDayTime      ? Vector<T> : never ;
    [Type.IntervalYearMonth    ]: T extends type.IntervalYearMonth    ? Vector<T> : never ;
    [Type.Map                  ]: T extends type.Map_                 ? Vector<T> : never ;
    [Type.List                 ]: T extends type.List                 ? Vector<T> : never ;
    [Type.Struct               ]: T extends type.Struct               ? Vector<T> : never ;
    [Type.Dictionary           ]: T extends type.Dictionary           ? Vector<T> : never ;
    [Type.FixedSizeList        ]: T extends type.FixedSizeList        ? Vector<T> : never ;
}[T['TType']];

/** @ignore */
export type TypeToDataType<T extends Type> = {
    [key: number               ]: type.DataType             ;
    [Type.Null                 ]: type.Null                 ;
    [Type.Bool                 ]: type.Bool                 ;
    [Type.Int                  ]: type.Int                  ;
    [Type.Int16                ]: type.Int16                ;
    [Type.Int32                ]: type.Int32                ;
    [Type.Int64                ]: type.Int64                ;
    [Type.Uint8                ]: type.Uint8                ;
    [Type.Uint16               ]: type.Uint16               ;
    [Type.Uint32               ]: type.Uint32               ;
    [Type.Uint64               ]: type.Uint64               ;
    [Type.Int8                 ]: type.Int8                 ;
    [Type.Float16              ]: type.Float16              ;
    [Type.Float32              ]: type.Float32              ;
    [Type.Float64              ]: type.Float64              ;
    [Type.Float                ]: type.Float                ;
    [Type.Utf8                 ]: type.Utf8                 ;
    [Type.Binary               ]: type.Binary               ;
    [Type.FixedSizeBinary      ]: type.FixedSizeBinary      ;
    [Type.Date                 ]: type.Date_                ;
    [Type.DateDay              ]: type.DateDay              ;
    [Type.DateMillisecond      ]: type.DateMillisecond      ;
    [Type.Timestamp            ]: type.Timestamp            ;
    [Type.TimestampSecond      ]: type.TimestampSecond      ;
    [Type.TimestampMillisecond ]: type.TimestampMillisecond ;
    [Type.TimestampMicrosecond ]: type.TimestampMicrosecond ;
    [Type.TimestampNanosecond  ]: type.TimestampNanosecond  ;
    [Type.Time                 ]: type.Time                 ;
    [Type.TimeSecond           ]: type.TimeSecond           ;
    [Type.TimeMillisecond      ]: type.TimeMillisecond      ;
    [Type.TimeMicrosecond      ]: type.TimeMicrosecond      ;
    [Type.TimeNanosecond       ]: type.TimeNanosecond       ;
    [Type.Decimal              ]: type.Decimal              ;
    [Type.Union                ]: type.Union                ;
    [Type.DenseUnion           ]: type.DenseUnion           ;
    [Type.SparseUnion          ]: type.SparseUnion          ;
    [Type.Interval             ]: type.Interval             ;
    [Type.IntervalDayTime      ]: type.IntervalDayTime      ;
    [Type.IntervalYearMonth    ]: type.IntervalYearMonth    ;
    [Type.Map                  ]: type.Map_                 ;
    [Type.List                 ]: type.List                 ;
    [Type.Struct               ]: type.Struct               ;
    [Type.Dictionary           ]: type.Dictionary           ;
    [Type.FixedSizeList        ]: type.FixedSizeList        ;
}[T];

/** @ignore */
type TypeToBuilder<T extends Type = any, TNull = any> = {
    [key: number               ]: builders.Builder                            ;
    [Type.Null                 ]: builders.NullBuilder<TNull>                 ;
    [Type.Bool                 ]: builders.BoolBuilder<TNull>                 ;
    [Type.Int8                 ]: builders.Int8Builder<TNull>                 ;
    [Type.Int16                ]: builders.Int16Builder<TNull>                ;
    [Type.Int32                ]: builders.Int32Builder<TNull>                ;
    [Type.Int64                ]: builders.Int64Builder<TNull>                ;
    [Type.Uint8                ]: builders.Uint8Builder<TNull>                ;
    [Type.Uint16               ]: builders.Uint16Builder<TNull>               ;
    [Type.Uint32               ]: builders.Uint32Builder<TNull>               ;
    [Type.Uint64               ]: builders.Uint64Builder<TNull>               ;
    [Type.Int                  ]: builders.IntBuilder<any, TNull>             ;
    [Type.Float16              ]: builders.Float16Builder<TNull>              ;
    [Type.Float32              ]: builders.Float32Builder<TNull>              ;
    [Type.Float64              ]: builders.Float64Builder<TNull>              ;
    [Type.Float                ]: builders.FloatBuilder<any, TNull>           ;
    [Type.Utf8                 ]: builders.Utf8Builder<TNull>                 ;
    [Type.Binary               ]: builders.BinaryBuilder<TNull>               ;
    [Type.FixedSizeBinary      ]: builders.FixedSizeBinaryBuilder<TNull>      ;
    [Type.Date                 ]: builders.DateBuilder<any, TNull>            ;
    [Type.DateDay              ]: builders.DateDayBuilder<TNull>              ;
    [Type.DateMillisecond      ]: builders.DateMillisecondBuilder<TNull>      ;
    [Type.Timestamp            ]: builders.TimestampBuilder<any, TNull>       ;
    [Type.TimestampSecond      ]: builders.TimestampSecondBuilder<TNull>      ;
    [Type.TimestampMillisecond ]: builders.TimestampMillisecondBuilder<TNull> ;
    [Type.TimestampMicrosecond ]: builders.TimestampMicrosecondBuilder<TNull> ;
    [Type.TimestampNanosecond  ]: builders.TimestampNanosecondBuilder<TNull>  ;
    [Type.Time                 ]: builders.TimeBuilder<any, TNull>            ;
    [Type.TimeSecond           ]: builders.TimeSecondBuilder<TNull>           ;
    [Type.TimeMillisecond      ]: builders.TimeMillisecondBuilder<TNull>      ;
    [Type.TimeMicrosecond      ]: builders.TimeMicrosecondBuilder<TNull>      ;
    [Type.TimeNanosecond       ]: builders.TimeNanosecondBuilder<TNull>       ;
    [Type.Decimal              ]: builders.DecimalBuilder<TNull>              ;
    [Type.Union                ]: builders.UnionBuilder<any, TNull>           ;
    [Type.DenseUnion           ]: builders.DenseUnionBuilder<any, TNull>      ;
    [Type.SparseUnion          ]: builders.SparseUnionBuilder<any, TNull>     ;
    [Type.Interval             ]: builders.IntervalBuilder<any, TNull>        ;
    [Type.IntervalDayTime      ]: builders.IntervalDayTimeBuilder<TNull>      ;
    [Type.IntervalYearMonth    ]: builders.IntervalYearMonthBuilder<TNull>    ;
    [Type.Map                  ]: builders.MapBuilder<any, any, TNull>        ;
    [Type.List                 ]: builders.ListBuilder<any, TNull>            ;
    [Type.Struct               ]: builders.StructBuilder<any, TNull>          ;
    [Type.Dictionary           ]: builders.DictionaryBuilder<any, TNull>      ;
    [Type.FixedSizeList        ]: builders.FixedSizeListBuilder<any, TNull>   ;
}[T];

/** @ignore */
type DataTypeToBuilder<T extends DataType = any, TNull = any> = {
    [key: number               ]:                                                                                                  builders.Builder<any, TNull> ;
    [Type.Null                 ]: T extends type.Null                 ? builders.NullBuilder<TNull>                              : builders.Builder<any, TNull> ;
    [Type.Bool                 ]: T extends type.Bool                 ? builders.BoolBuilder<TNull>                              : builders.Builder<any, TNull> ;
    [Type.Int8                 ]: T extends type.Int8                 ? builders.Int8Builder<TNull>                              : builders.Builder<any, TNull> ;
    [Type.Int16                ]: T extends type.Int16                ? builders.Int16Builder<TNull>                             : builders.Builder<any, TNull> ;
    [Type.Int32                ]: T extends type.Int32                ? builders.Int32Builder<TNull>                             : builders.Builder<any, TNull> ;
    [Type.Int64                ]: T extends type.Int64                ? builders.Int64Builder<TNull>                             : builders.Builder<any, TNull> ;
    [Type.Uint8                ]: T extends type.Uint8                ? builders.Uint8Builder<TNull>                             : builders.Builder<any, TNull> ;
    [Type.Uint16               ]: T extends type.Uint16               ? builders.Uint16Builder<TNull>                            : builders.Builder<any, TNull> ;
    [Type.Uint32               ]: T extends type.Uint32               ? builders.Uint32Builder<TNull>                            : builders.Builder<any, TNull> ;
    [Type.Uint64               ]: T extends type.Uint64               ? builders.Uint64Builder<TNull>                            : builders.Builder<any, TNull> ;
    [Type.Int                  ]: T extends type.Int                  ? builders.IntBuilder<T, TNull>                            : builders.Builder<any, TNull> ;
    [Type.Float16              ]: T extends type.Float16              ? builders.Float16Builder<TNull>                           : builders.Builder<any, TNull> ;
    [Type.Float32              ]: T extends type.Float32              ? builders.Float32Builder<TNull>                           : builders.Builder<any, TNull> ;
    [Type.Float64              ]: T extends type.Float64              ? builders.Float64Builder<TNull>                           : builders.Builder<any, TNull> ;
    [Type.Float                ]: T extends type.Float                ? builders.FloatBuilder<T, TNull>                          : builders.Builder<any, TNull> ;
    [Type.Utf8                 ]: T extends type.Utf8                 ? builders.Utf8Builder<TNull>                              : builders.Builder<any, TNull> ;
    [Type.Binary               ]: T extends type.Binary               ? builders.BinaryBuilder<TNull>                            : builders.Builder<any, TNull> ;
    [Type.FixedSizeBinary      ]: T extends type.FixedSizeBinary      ? builders.FixedSizeBinaryBuilder<TNull>                   : builders.Builder<any, TNull> ;
    [Type.Date                 ]: T extends type.Date_                ? builders.DateBuilder<T, TNull>                           : builders.Builder<any, TNull> ;
    [Type.DateDay              ]: T extends type.DateDay              ? builders.DateDayBuilder<TNull>                           : builders.Builder<any, TNull> ;
    [Type.DateMillisecond      ]: T extends type.DateMillisecond      ? builders.DateMillisecondBuilder<TNull>                   : builders.Builder<any, TNull> ;
    [Type.Timestamp            ]: T extends type.Timestamp            ? builders.TimestampBuilder<T, TNull>                      : builders.Builder<any, TNull> ;
    [Type.TimestampSecond      ]: T extends type.TimestampSecond      ? builders.TimestampSecondBuilder<TNull>                   : builders.Builder<any, TNull> ;
    [Type.TimestampMillisecond ]: T extends type.TimestampMillisecond ? builders.TimestampMillisecondBuilder<TNull>              : builders.Builder<any, TNull> ;
    [Type.TimestampMicrosecond ]: T extends type.TimestampMicrosecond ? builders.TimestampMicrosecondBuilder<TNull>              : builders.Builder<any, TNull> ;
    [Type.TimestampNanosecond  ]: T extends type.TimestampNanosecond  ? builders.TimestampNanosecondBuilder<TNull>               : builders.Builder<any, TNull> ;
    [Type.Time                 ]: T extends type.Time                 ? builders.TimeBuilder<T, TNull>                           : builders.Builder<any, TNull> ;
    [Type.TimeSecond           ]: T extends type.TimeSecond           ? builders.TimeSecondBuilder<TNull>                        : builders.Builder<any, TNull> ;
    [Type.TimeMillisecond      ]: T extends type.TimeMillisecond      ? builders.TimeMillisecondBuilder<TNull>                   : builders.Builder<any, TNull> ;
    [Type.TimeMicrosecond      ]: T extends type.TimeMicrosecond      ? builders.TimeMicrosecondBuilder<TNull>                   : builders.Builder<any, TNull> ;
    [Type.TimeNanosecond       ]: T extends type.TimeNanosecond       ? builders.TimeNanosecondBuilder<TNull>                    : builders.Builder<any, TNull> ;
    [Type.Decimal              ]: T extends type.Decimal              ? builders.DecimalBuilder<TNull>                           : builders.Builder<any, TNull> ;
    [Type.Union                ]: T extends type.Union                ? builders.UnionBuilder<T, TNull>                          : builders.Builder<any, TNull> ;
    [Type.DenseUnion           ]: T extends type.DenseUnion           ? builders.DenseUnionBuilder<T, TNull>                     : builders.Builder<any, TNull> ;
    [Type.SparseUnion          ]: T extends type.SparseUnion          ? builders.SparseUnionBuilder<T, TNull>                    : builders.Builder<any, TNull> ;
    [Type.Interval             ]: T extends type.Interval             ? builders.IntervalBuilder<T, TNull>                       : builders.Builder<any, TNull> ;
    [Type.IntervalDayTime      ]: T extends type.IntervalDayTime      ? builders.IntervalDayTimeBuilder<TNull>                   : builders.Builder<any, TNull> ;
    [Type.IntervalYearMonth    ]: T extends type.IntervalYearMonth    ? builders.IntervalYearMonthBuilder<TNull>                 : builders.Builder<any, TNull> ;
    [Type.Map                  ]: T extends type.Map_                 ? builders.MapBuilder<T['keyType'], T['valueType'], TNull> : builders.Builder<any, TNull> ;
    [Type.List                 ]: T extends type.List                 ? builders.ListBuilder<T['valueType'], TNull>              : builders.Builder<any, TNull> ;
    [Type.Struct               ]: T extends type.Struct               ? builders.StructBuilder<T['dataTypes'], TNull>            : builders.Builder<any, TNull> ;
    [Type.Dictionary           ]: T extends type.Dictionary           ? builders.DictionaryBuilder<T, TNull>                     : builders.Builder<any, TNull> ;
    [Type.FixedSizeList        ]: T extends type.FixedSizeList        ? builders.FixedSizeListBuilder<T['valueType'], TNull>     : builders.Builder<any, TNull> ;
}[T['TType']];
