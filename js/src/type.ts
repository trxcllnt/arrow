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

import { Field } from './schema';
import { flatbuffers } from 'flatbuffers';
import { Vector, ArrayBufferViewConstructor, VectorLike } from './interfaces';

import Long = flatbuffers.Long;
import {
    Type, ArrowType,
    Precision, UnionMode,
    DateUnit, TimeUnit, IntervalUnit
} from './enum';

export type TimeBitWidth = 32 | 64;
export type IntBitWidth = 8 | 16 | 32 | 64;
export type IsSigned = { 'true': true; 'false': false };

export type Row<T extends { [key: string]: DataType; }> =
      { readonly length: number }
    & ( Iterable<T[keyof T]['TValue']> )
    & { [P in keyof T]: T[P]['TValue'] }
    & { get<K extends keyof T>(key: K): T[K]['TValue']; }
    ;

export interface DataType<TType extends Type = Type> {
    readonly TType: TType;
    readonly TArray: any;
    readonly TValue: any;
    readonly ArrayType: any;
}

export class DataType<TType extends Type = Type, TChildren extends { [key: string]: DataType } = any> {

    // @ts-ignore
    public [Symbol.toStringTag]: string;

    static            isNull (x: any): x is Null            { return x && x.TType === Type.Null;            }
    static             isInt (x: any): x is Int             { return x && x.TType === Type.Int;             }
    static           isFloat (x: any): x is Float           { return x && x.TType === Type.Float;           }
    static          isBinary (x: any): x is Binary          { return x && x.TType === Type.Binary;          }
    static            isUtf8 (x: any): x is Utf8            { return x && x.TType === Type.Utf8;            }
    static            isBool (x: any): x is Bool            { return x && x.TType === Type.Bool;            }
    static         isDecimal (x: any): x is Decimal         { return x && x.TType === Type.Decimal;         }
    static            isDate (x: any): x is Date_           { return x && x.TType === Type.Date;            }
    static            isTime (x: any): x is Time            { return x && x.TType === Type.Time;            }
    static       isTimestamp (x: any): x is Timestamp       { return x && x.TType === Type.Timestamp;       }
    static        isInterval (x: any): x is Interval        { return x && x.TType === Type.Interval;        }
    static            isList (x: any): x is List            { return x && x.TType === Type.List;            }
    static          isStruct (x: any): x is Struct          { return x && x.TType === Type.Struct;          }
    static           isUnion (x: any): x is Union           { return x && x.TType === Type.Union;           }
    static isFixedSizeBinary (x: any): x is FixedSizeBinary { return x && x.TType === Type.FixedSizeBinary; }
    static   isFixedSizeList (x: any): x is FixedSizeList   { return x && x.TType === Type.FixedSizeList;   }
    static             isMap (x: any): x is Map_            { return x && x.TType === Type.Map;             }
    static      isDictionary (x: any): x is Dictionary      { return x && x.TType === Type.Dictionary;      }

    constructor(public readonly TType: TType = <any> Type.NONE,
                public readonly children?: Field<TChildren[keyof TChildren]>[]
                ) {}

    protected static [Symbol.toStringTag] = ((proto: DataType) => {
        (<any> proto).ArrayType = Array;
        return proto[Symbol.toStringTag] = 'DataType';
    })(DataType.prototype);
}

export interface Null extends DataType<Type.Null> { TArray: void; TValue: null; }
export class Null extends DataType<Type.Null> {
    constructor() {
        super(Type.Null);
    }
    public toString() { return `Null`; }
    protected static [Symbol.toStringTag] = ((proto: Null) => {
        return proto[Symbol.toStringTag] = 'Null';
    })(Null.prototype);
}

type Ints = Type.Int | Type.Int8 | Type.Int16 | Type.Int32 | Type.Int64 | Type.Uint8 | Type.Uint16 | Type.Uint32 | Type.Uint64;
type IType = {
    [Type.Int   ]: { bitWidth: any; isSigned: any;   TArray: IntArray;    TValue: number | IntArray; };
    [Type.Int8  ]: { bitWidth:   8; isSigned: true;  TArray: Int8Array;   TValue: number;            };
    [Type.Int16 ]: { bitWidth:  16; isSigned: true;  TArray: Int16Array;  TValue: number;            };
    [Type.Int32 ]: { bitWidth:  32; isSigned: true;  TArray: Int32Array;  TValue: number;            };
    [Type.Int64 ]: { bitWidth:  64; isSigned: true;  TArray: Int32Array;  TValue: Int32Array;        };
    [Type.Uint8 ]: { bitWidth:   8; isSigned: false; TArray: Uint8Array;  TValue: number;            };
    [Type.Uint16]: { bitWidth:  16; isSigned: false; TArray: Uint16Array; TValue: number;            };
    [Type.Uint32]: { bitWidth:  32; isSigned: false; TArray: Uint32Array; TValue: number;            };
    [Type.Uint64]: { bitWidth:  64; isSigned: false; TArray: Uint32Array; TValue: Uint32Array;       };
};

export interface Int<T extends Ints = Ints> extends DataType<T> { TArray: IType[T]['TArray']; TValue: IType[T]['TValue']; }
export class Int<T extends Ints = Ints> extends DataType<T> {
    constructor(public readonly isSigned: IType[T]['isSigned'],
                public readonly bitWidth: IType[T]['bitWidth']) {
        super(Type.Int as T);
    }
    public get ArrayType(): ArrayBufferViewConstructor<IType[T]['TArray']> {
        switch (this.bitWidth) {
            case  8: return (this.isSigned ?  Int8Array :  Uint8Array) as any;
            case 16: return (this.isSigned ? Int16Array : Uint16Array) as any;
            case 32: return (this.isSigned ? Int32Array : Uint32Array) as any;
            case 64: return (this.isSigned ? Int32Array : Uint32Array) as any;
        }
        throw new Error(`Unrecognized ${this[Symbol.toStringTag]} type`);
    }
    public toString() { return `${this.isSigned ? `I` : `Ui`}nt${this.bitWidth}`; }
    protected static [Symbol.toStringTag] = ((proto: Int) => {
        return proto[Symbol.toStringTag] = 'Int';
    })(Int.prototype);
}

export class Int8 extends Int<Type.Int8> { constructor() { super(true, 8); } }
export class Int16 extends Int<Type.Int16> { constructor() { super(true, 16); } }
export class Int32 extends Int<Type.Int32> { constructor() { super(true, 32); } }
export class Int64 extends Int<Type.Int64> { constructor() { super(true, 64); } }
export class Uint8 extends Int<Type.Uint8> { constructor() { super(false, 8); } }
export class Uint16 extends Int<Type.Uint16> { constructor() { super(false, 16); } }
export class Uint32 extends Int<Type.Uint32> { constructor() { super(false, 32); } }
export class Uint64 extends Int<Type.Uint64> { constructor() { super(false, 64); } }

type Floats = Type.Float | Type.Float16 | Type.Float32 | Type.Float64;
type FType = {
    [Type.Float  ]: { precision: Precision;        TArray: FloatArray;    TValue: number; };
    [Type.Float16]: { precision: Precision.HALF;   TArray: Uint16Array;   TValue: number; };
    [Type.Float32]: { precision: Precision.SINGLE; TArray: Float32Array;  TValue: number; };
    [Type.Float64]: { precision: Precision.DOUBLE; TArray: Float32Array;  TValue: number; };
};

export interface Float<T extends Floats = Floats> extends DataType<T> { TArray: FType[T]['TArray']; TValue: number; }
export class Float<T extends Floats = Floats> extends DataType<T> {
    constructor(public readonly precision: Precision) {
        super(Type.Float as T);
    }
    // @ts-ignore
    public get ArrayType(): ArrayBufferViewConstructor<FType[T]['TArray']> {
        switch (this.precision) {
            case Precision.HALF: return Uint16Array as any;
            case Precision.SINGLE: return Float32Array as any;
            case Precision.DOUBLE: return Float64Array as any;
        }
        throw new Error(`Unrecognized ${this[Symbol.toStringTag]} type`);
    }
    public toString() { return `Float${(this.precision << 5) || 16}`; }
    protected static [Symbol.toStringTag] = ((proto: Float) => {
        return proto[Symbol.toStringTag] = 'Float';
    })(Float.prototype);
}

export class Float16 extends Float<Type.Float16> { constructor() { super(Precision.HALF); } }
export class Float32 extends Float<Type.Float32> { constructor() { super(Precision.SINGLE); } }
export class Float64 extends Float<Type.Float64> { constructor() { super(Precision.DOUBLE); } }

export interface Binary extends DataType<Type.Binary> { TArray: Uint8Array; TValue: Uint8Array; }
export class Binary extends DataType<Type.Binary> {
    constructor() {
        super(Type.Binary);
    }
    public toString() { return `Binary`; }
    protected static [Symbol.toStringTag] = ((proto: Binary) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'Binary';
    })(Binary.prototype);
}

export interface Utf8 extends DataType<Type.Utf8> { TArray: Uint8Array; TValue: string; ArrayType: typeof Uint8Array; }
export class Utf8 extends DataType<Type.Utf8> {
    constructor() {
        super(Type.Utf8);
    }
    public toString() { return `Utf8`; }
    protected static [Symbol.toStringTag] = ((proto: Utf8) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'Utf8';
    })(Utf8.prototype);
}

export interface Bool extends DataType<Type.Bool> { TArray: Uint8Array; TValue: boolean; ArrayType: typeof Uint8Array; }
export class Bool extends DataType<Type.Bool> {
    constructor() {
        super(Type.Bool);
    }
    public toString() { return `Bool`; }
    protected static [Symbol.toStringTag] = ((proto: Bool) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'Bool';
    })(Bool.prototype);
}

export interface Decimal extends DataType<Type.Decimal> { TArray: Uint32Array; TValue: Uint32Array; ArrayType: typeof Uint32Array; }
export class Decimal extends DataType<Type.Decimal> {
    constructor(public readonly scale: number,
                public readonly precision: number) {
        super(Type.Decimal);
    }
    public toString() { return `Decimal[${this.precision}e${this.scale > 0 ? `+` : ``}${this.scale}]`; }
    protected static [Symbol.toStringTag] = ((proto: Decimal) => {
        (<any> proto).ArrayType = Uint32Array;
        return proto[Symbol.toStringTag] = 'Decimal';
    })(Decimal.prototype);
}

export type Dates = Type.Date | Type.DateDay | Type.DateMillisecond;
/* tslint:disable:class-name */
export interface Date_<T extends Dates = Dates> extends DataType<T> { TArray: Int32Array; TValue: Date; ArrayType: typeof Int32Array; }
export class Date_<T extends Dates = Dates> extends DataType<T> {
    constructor(public readonly unit: DateUnit) {
        super(Type.Date as T);
    }
    public toString() { return `Date${(this.unit + 1) * 32}<${DateUnit[this.unit]}>`; }
    protected static [Symbol.toStringTag] = ((proto: Date_) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Date';
    })(Date_.prototype);
}

export class DateDay extends Date_<Type.DateDay> { constructor() { super(DateUnit.DAY); } }
export class DateMillisecond extends Date_<Type.DateMillisecond> { constructor() { super(DateUnit.MILLISECOND); } }

type Times = Type.Time | Type.TimeSecond | Type.TimeMillisecond | Type.TimeMicrosecond | Type.TimeNanosecond;
type TimesType = {
    [Type.Time           ]: { unit: TimeUnit;             TValue: number | Uint32Array };
    [Type.TimeSecond     ]: { unit: TimeUnit.SECOND;      TValue: number;              };
    [Type.TimeMillisecond]: { unit: TimeUnit.MILLISECOND; TValue: number;              };
    [Type.TimeMicrosecond]: { unit: TimeUnit.MICROSECOND; TValue: Uint32Array;         };
    [Type.TimeNanosecond ]: { unit: TimeUnit.NANOSECOND;  TValue: Uint32Array;         };
};

export interface Time<T extends Times = Times> extends DataType<T> { TArray: Uint32Array; TValue: TimesType[T]['TValue']; ArrayType: typeof Uint32Array; }
export class Time<T extends Times = Times> extends DataType<T> {
    constructor(public readonly unit: TimesType[T]['unit'],
                public readonly bitWidth: TimeBitWidth) {
        super(Type.Time as T);
    }
    public toString() { return `Time${this.bitWidth}<${TimeUnit[this.unit]}>`; }
    protected static [Symbol.toStringTag] = ((proto: Time) => {
        (<any> proto).ArrayType = Uint32Array;
        return proto[Symbol.toStringTag] = 'Time';
    })(Time.prototype);
}

export class TimeSecond extends Time<Type.TimeSecond> { constructor(bitWidth: TimeBitWidth) { super(TimeUnit.SECOND, bitWidth); } }
export class TimeMillisecond extends Time<Type.TimeMillisecond> { constructor(bitWidth: TimeBitWidth) { super(TimeUnit.MILLISECOND, bitWidth); } }
export class TimeMicrosecond extends Time<Type.TimeMicrosecond> { constructor(bitWidth: TimeBitWidth) { super(TimeUnit.MICROSECOND, bitWidth); } }
export class TimeNanosecond extends Time<Type.TimeNanosecond> { constructor(bitWidth: TimeBitWidth) { super(TimeUnit.NANOSECOND, bitWidth); } }

type Timestamps = Type.Timestamp | Type.TimestampSecond | Type.TimestampMillisecond | Type.TimestampMicrosecond | Type.TimestampNanosecond;
export interface Timestamp<T extends Timestamps = Timestamps> extends DataType<T> { TArray: Int32Array; TValue: number; ArrayType: typeof Int32Array; }
export class Timestamp<T extends Timestamps = Timestamps> extends DataType<T> {
    constructor(public unit: TimeUnit, public timezone?: string | null) {
        super(Type.Timestamp as T);
    }
    public toString() { return `Timestamp<${TimeUnit[this.unit]}${this.timezone ? `, ${this.timezone}` : ``}>`; }
    protected static [Symbol.toStringTag] = ((proto: Timestamp) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Timestamp';
    })(Timestamp.prototype);
}

export class TimestampSecond extends Timestamp<Type.TimestampSecond> { constructor(timezone?: string | null) { super(TimeUnit.SECOND, timezone); } }
export class TimestampMillisecond extends Timestamp<Type.TimestampMillisecond> { constructor(timezone?: string | null) { super(TimeUnit.MILLISECOND, timezone); } }
export class TimestampMicrosecond extends Timestamp<Type.TimestampMicrosecond> { constructor(timezone?: string | null) { super(TimeUnit.MICROSECOND, timezone); } }
export class TimestampNanosecond extends Timestamp<Type.TimestampNanosecond> { constructor(timezone?: string | null) { super(TimeUnit.NANOSECOND, timezone); } }

type Intervals = Type.Interval | Type.IntervalDayTime | Type.IntervalYearMonth;
export interface Interval<T extends Intervals = Intervals> extends DataType<T> { TArray: Int32Array; TValue: Int32Array; ArrayType: typeof Int32Array; }
export class Interval<T extends Intervals = Intervals> extends DataType<T> {
    constructor(public unit: IntervalUnit) {
        super(Type.Interval as T);
    }
    public toString() { return `Interval<${IntervalUnit[this.unit]}>`; }
    protected static [Symbol.toStringTag] = ((proto: Interval) => {
        (<any> proto).ArrayType = Int32Array;
        return proto[Symbol.toStringTag] = 'Interval';
    })(Interval.prototype);
}

export class IntervalDayTime extends Interval<Type.IntervalDayTime> { constructor() { super(IntervalUnit.DAY_TIME); } }
export class IntervalYearMonth extends Interval<Type.IntervalYearMonth> { constructor() { super(IntervalUnit.YEAR_MONTH); } }

export interface List<T extends DataType = any> extends DataType<Type.List>  { TArray: IterableArrayLike<T>; TValue: Vector<T>; }
export class List<T extends DataType = any> extends DataType<Type.List, { [0]: T }> {
    constructor(public children: Field<T>[]) {
        super(Type.List, children);
    }
    public toString() { return `List<${this.valueType}>`; }
    public get valueType(): T { return this.children[0].type as T; }
    public get valueField(): Field<T> { return this.children[0] as Field<T>; }
    public get ArrayType(): T['ArrayType'] { return this.valueType.ArrayType; }
    protected static [Symbol.toStringTag] = ((proto: List) => {
        return proto[Symbol.toStringTag] = 'List';
    })(List.prototype);
}

export interface Struct<T extends { [key: string]: DataType; } = any> extends DataType<Type.Struct> { TArray: IterableArrayLike<Row<T>>; TValue: Row<T>; dataTypes: T; }
export class Struct<T extends { [key: string]: DataType; } = any> extends DataType<Type.Struct, T> {
    constructor(public children: Field<T[keyof T]>[]) {
        super(Type.Struct, children);
    }
    public toString() { return `Struct<${this.children.map((f) => f.type).join(`, `)}>`; }
    protected static [Symbol.toStringTag] = ((proto: Struct) => {
        return proto[Symbol.toStringTag] = 'Struct';
    })(Struct.prototype);
}

type Unions = Type.Union | Type.DenseUnion | Type.SparseUnion;
export interface Union<T extends Unions = Unions> extends DataType<T> { TArray: Int8Array; TValue: any[]; }
export class Union<T extends Unions = Unions> extends DataType<T> {
    public readonly typeIdToChildIndex: Record<Type, number>;
    constructor(public readonly mode: UnionMode,
                public readonly typeIds: ArrowType[],
                public readonly children: Field[]) {
        super(Type.Union as T, children);
        this.typeIdToChildIndex = (typeIds || []).reduce((typeIdToChildIndex, typeId, idx) => {
            return (typeIdToChildIndex[typeId] = idx) && typeIdToChildIndex || typeIdToChildIndex;
        }, Object.create(null) as Record<Type, number>);
    }
    public toString() { return `${this[Symbol.toStringTag]}<${
        this.children.map((x) => `${x.type}`).join(` | `)
    }>`; }
    protected static [Symbol.toStringTag] = ((proto: Union) => {
        (<any> proto).ArrayType = Int8Array;
        return proto[Symbol.toStringTag] = 'Union';
    })(Union.prototype);
}

export class DenseUnion extends Union<Type.DenseUnion> {
    constructor(typeIds: ArrowType[], children: Field[]) {
        super(UnionMode.Dense, typeIds, children);
    }
}

export class SparseUnion extends Union<Type.SparseUnion> {
    constructor(typeIds: ArrowType[], children: Field[]) {
        super(UnionMode.Sparse, typeIds, children);
    }
}

export interface FixedSizeBinary extends DataType<Type.FixedSizeBinary> { TArray: Uint8Array; TValue: Uint8Array; ArrayType: typeof Uint8Array; }
export class FixedSizeBinary extends DataType<Type.FixedSizeBinary> {
    constructor(public readonly byteWidth: number) {
        super(Type.FixedSizeBinary);
    }
    public toString() { return `FixedSizeBinary[${this.byteWidth}]`; }
    protected static [Symbol.toStringTag] = ((proto: FixedSizeBinary) => {
        (<any> proto).ArrayType = Uint8Array;
        return proto[Symbol.toStringTag] = 'FixedSizeBinary';
    })(FixedSizeBinary.prototype);
}

export interface FixedSizeList<T extends DataType = any> extends DataType<Type.FixedSizeList> { TArray: IterableArrayLike<T['TArray']>; TValue: Vector<T>; }
export class FixedSizeList<T extends DataType = any> extends DataType<Type.FixedSizeList, { [0]: T }> {
    constructor(public readonly listSize: number,
                public readonly children: Field<T>[]) {
        super(Type.FixedSizeList, children);
    }
    public get valueType(): T { return this.children[0].type as T; }
    public get valueField(): Field<T> { return this.children[0] as Field<T>; }
    public get ArrayType(): T['ArrayType'] { return this.valueType.ArrayType; }
    public toString() { return `FixedSizeList[${this.listSize}]<${this.valueType}>`; }
    protected static [Symbol.toStringTag] = ((proto: FixedSizeList) => {
        return proto[Symbol.toStringTag] = 'FixedSizeList';
    })(FixedSizeList.prototype);
}

/* tslint:disable:class-name */
export interface Map_<T extends { [key: string]: DataType; } = any> extends DataType<Type.Map> { TArray: Uint8Array; TValue: Row<T>; dataTypes: T; }
export class Map_<T extends { [key: string]: DataType; } = any> extends DataType<Type.Map, T> {
    constructor(public readonly children: Field<T[keyof T]>[],
                public readonly keysSorted: boolean = false) {
        super(Type.Map, children);
    }
    public toString() { return `Map<${this.children.join(`, `)}>`; }
    protected static [Symbol.toStringTag] = ((proto: Map_) => {
        return proto[Symbol.toStringTag] = 'Map_';
    })(Map_.prototype);
}

const getId = ((atomicDictionaryId) => () => ++atomicDictionaryId)(-1);

export interface Dictionary<T extends DataType = any, TKey extends Int = Int32> extends DataType<Type.Dictionary> { TArray: TKey['TArray']; TValue: T['TValue']; }
export class Dictionary<T extends DataType = any, TKey extends Int = Int32> extends DataType<Type.Dictionary> {
    public readonly id: number;
    public readonly indices: TKey;
    public readonly dictionary: T;
    public readonly isOrdered: boolean;
    // @ts-ignore;
    public dictionaryVector: VectorLike<T>;
    constructor(dictionary: T, indices: TKey, id?: Long | number | null, isOrdered?: boolean | null) {
        super(Type.Dictionary);
        this.indices = indices;
        this.dictionary = dictionary;
        this.isOrdered = isOrdered || false;
        this.id = id == null ? getId() : typeof id === 'number' ? id : id.low;
    }
    public set children(_: T['children']) {}
    public get children() { return this.dictionary.children; }
    public get valueType(): T { return this.dictionary as T; }
    public get ArrayType(): T['ArrayType'] { return this.dictionary.ArrayType; }
    public toString() { return `Dictionary<${this.indices}, ${this.dictionary}>`; }
    protected static [Symbol.toStringTag] = ((proto: Dictionary) => {
        return proto[Symbol.toStringTag] = 'Dictionary';
    })(Dictionary.prototype);
}

export interface IterableArrayLike<T = any> extends ArrayLike<T>, Iterable<T> {}
export type FloatArray = Uint16Array | Float32Array | Float64Array;
export type IntArray = Int8Array | Int16Array | Int32Array | Uint8Array | Uint16Array | Uint32Array;
