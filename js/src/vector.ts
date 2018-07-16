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
import { GetVisitor } from './visitor/get';
import { IndexOfVisitor } from './visitor/indexof';
import { IteratorVisitor } from './visitor/iterator';
import { TypeToVectorVisitor } from './visitor/vectortypes';
import {
    IterableArrayLike,
    Type, DType, DataType, Dictionary,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Map_, Struct, Union,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp,
    
    DenseUnion, SparseUnion,
    DateDay, DateMillisecond,
    IntervalDayTime, IntervalYearMonth,
    TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Float16, Float32, Float64,
} from './type';

const getVisitor = new GetVisitor();
const indexOfVisitor = new IndexOfVisitor();
const iteratorVisitor = new IteratorVisitor();
const getVectorCtor = new TypeToVectorVisitor();
const getVectorConstructor = <T extends Type>(type: T) => getVectorCtor.visit(type);

export interface Vector<T extends DataType = any> {
    toJSON(): any;
    isValid(index: number): boolean;
    get(index: number): T['TValue'] | null;
    toArray(): IterableArrayLike<T['TValue'] | null>;
    indexOf(element: T['TValue'] | null, index?: number): number;
    [Symbol.iterator](): IterableIterator<T['TValue'] | null>;
}

export class Vector<T extends DataType = any> {

    static new <T extends DataType>(data: Data<T>, ...args: any[]) {
        return new Vector<T>(data, ...args) as VType[T['TType']];
    }
    
    public readonly data: Data<T>;
    public readonly stride: number;
    public readonly numChildren: number;
    protected _children: Vector[];

    constructor(data: Data<T>, children?: Vector[], stride?: number) {
        const VCtor = getVectorConstructor(data.TType);
        // Return the correct Vector subclass based on the Arrow Type
        if (!(this instanceof VCtor)) {
            return Reflect.construct(Vector, arguments, VCtor);
        }
        this.bindDataAccessors(this.data = data);
        this.numChildren = data.childData.length;
        this.stride = Math.floor(Math.max(stride || 1, 1));
        this._children = children || new Array(this.numChildren);
    }

    // @ts-ignore
    protected bindDataAccessors(data: Data<T>) {}
    
    public get type() { return this.data.type; }
    public get TType() { return this.data.TType; }
    public get length() { return this.data.length; }
    public get offset() { return this.data.offset; }
    public get values() { return this.data.values; }
    public get typeIds() { return this.data.typeIds; }
    public get nullBitmap() { return this.data.nullBitmap; }
    public get valueOffsets() { return this.data.valueOffsets; }
    public get nullCount() { return this.data.nullCount; }
    public get VectorName() { return this.constructor.name; }
    public get [Symbol.toStringTag]() { return `${this.VectorName}<${this.type[Symbol.toStringTag]}>`; }
    public clone<R extends DataType = T>(data: Data<R>): VType[R['TType']] { return Vector.new<R>(data); }
    public getChildAt<R extends DataType = any>(index: number): Vector<R> | null {
        return index < 0 || index >= this.numChildren
            ? null
            : (this._children[index] as Vector<R>) ||
              (this._children[index] = Vector.new<R>(this.data.childData[index] as Data<R>));
    }

    public toJSON(): any {}
    public slice(begin?: number, end?: number) {
        // Adjust args similar to Array.prototype.slice. Normalize begin/end to
        // clamp between 0 and length, and wrap around on negative indices, e.g.
        // slice(-1, 5) or slice(5, -1)
        let { length, stride } = this;
        let total = length, from = (begin || 0) * stride;
        let to = (typeof end === 'number' ? end : total) * stride;
        if (to < 0) { to = total - (to * -1) % total; }
        if (from < 0) { from = total - (from * -1) % total; }
        if (to < from) { [from, to] = [to, from]; }
        total = !isFinite(total = (to - from)) || total < 0 ? 0 : total;
        return this.clone(this.data.slice(from, Math.min(total, length * stride)));
    }
}

export class NullVector extends Vector<Null> {}

export class IntVector<T extends Int = any> extends Vector<T> {}
export class Int8Vector extends IntVector<Int8> {}
export class Int16Vector extends IntVector<Int16> {}
export class Int32Vector extends IntVector<Int32> {}
export class Int64Vector extends IntVector<Int64> {}
export class Uint8Vector extends IntVector<Uint8> {}
export class Uint16Vector extends IntVector<Uint16> {}
export class Uint32Vector extends IntVector<Uint32> {}
export class Uint64Vector extends IntVector<Uint64> {}

export class FloatVector<T extends Float = any> extends Vector<T> {}
export class Float16Vector extends FloatVector<Float16> {}
export class Float32Vector extends FloatVector<Float32> {}
export class Float64Vector extends FloatVector<Float64> {}

export class BinaryVector extends Vector<Binary> {
    public asUtf8() {
        return Vector.new(this.data.clone(new Utf8()));
    }
}

export class Utf8Vector extends Vector<Utf8> {
    public asBinary() {
        return Vector.new(this.data.clone(new Binary()));
    }
}

export class BoolVector extends Vector<Bool> {}

export class DecimalVector extends Vector<Decimal> {}

export class DateVector<T extends Date_ = Date_> extends Vector<T> {}
export class DateDayVector extends DateVector<DateDay> {}
export class DateMillisecondVector extends DateVector<DateMillisecond> {}

export class TimeVector<T extends Time = Time> extends Vector<T> {}
export class TimeSecondVector extends TimeVector<TimeSecond> {}
export class TimeMillisecondVector extends TimeVector<TimeMillisecond> {}
export class TimeMicrosecondVector extends TimeVector<TimeMicrosecond> {}
export class TimeNanosecondVector extends TimeVector<TimeNanosecond> {}

export class TimestampVector<T extends Timestamp = Timestamp> extends Vector<T> {}
export class TimestampSecondVector extends TimestampVector<TimestampSecond> {}
export class TimestampMillisecondVector extends TimestampVector<TimestampMillisecond> {}
export class TimestampMicrosecondVector extends TimestampVector<TimestampMicrosecond> {}
export class TimestampNanosecondVector extends TimestampVector<TimestampNanosecond> {}

export class IntervalVector<T extends Interval = Interval> extends Vector<T> {}
export class IntervalDayTimeVector extends IntervalVector<IntervalDayTime> {}
export class IntervalYearMonthVector extends IntervalVector<IntervalYearMonth> {}

export class ListVector<T extends DataType = any> extends Vector<List<T>> {}

export class StructVector extends Vector<Struct> {
    public asMap(keysSorted: boolean = false) {
        return Vector.new(this.data.clone(new Map_(keysSorted, this.type.children)));
    }
}

export class UnionVector<T extends Union = Union> extends Vector<T> {
    public get typeIdToChildIndex() { return this.type.typeIdToChildIndex; }
}

export class DenseUnionVector extends UnionVector<DenseUnion> {
    public get valueOffsets() { return this.data.valueOffsets!; }
}

export class SparseUnionVector extends UnionVector<SparseUnion> {}

export class FixedSizeBinaryVector extends Vector<FixedSizeBinary> {
    constructor(data: Data<FixedSizeBinary>) {
        super(data, void 0, data.type.byteWidth);
    }
}

export class FixedSizeListVector<T extends DataType = any> extends Vector<FixedSizeList<T>> {
    constructor(data: Data<FixedSizeList<T>>) {
        super(data, void 0, data.type.listSize);
    }
}

export class MapVector extends Vector<Map_> {
    public asStruct() {
        return Vector.new(this.data.clone(new Struct(this.type.children)));
    }
}

export class DictionaryVector<T extends DataType = any> extends Vector<Dictionary<T>> {
    public readonly indices: Vector<Int>;
    public readonly dictionary: Vector<T>;
    constructor(dictionary: Vector<T>, indices: Vector<Int>) {
        super(indices.data as Data<any>);
        this.indices = indices;
        this.dictionary = dictionary;
    }
    public getKey(index: number) { return this.indices.get(index); }
    public getValue(key: number) { return this.dictionary.get(key); }
    public reverseLookup(value: T) { return this.dictionary.indexOf(value); }
}

// Bind and assign the Operator Visitor methods to each of the Vector subclasses for each Type
(Object.keys(Type) as any[])
    .filter((T: any): T is Type => typeof Type[T] === 'number')
    .filter((TType) => TType !== Type.NONE)
    .forEach((TType) => {
        let typeIds: Type[];
        switch (TType) {
            case Type.Int:       typeIds = [Type.Int8, Type.Int16, Type.Int32, Type.Int64, Type.Uint8, Type.Uint16, Type.Uint32, Type.Uint64]; break;
            case Type.Float:     typeIds = [Type.Float16, Type.Float32, Type.Float64]; break;
            case Type.Date:      typeIds = [Type.DateDay, Type.DateMillisecond]; break;
            case Type.Time:      typeIds = [Type.TimeSecond, Type.TimeMillisecond, Type.TimeMicrosecond, Type.TimeNanosecond]; break;
            case Type.Timestamp: typeIds = [Type.TimestampSecond, Type.TimestampMillisecond, Type.TimestampMicrosecond, Type.TimestampNanosecond]; break;
            case Type.Interval:  typeIds = [Type.IntervalDayTime, Type.IntervalYearMonth]; break;
            case Type.Union:     typeIds = [Type.DenseUnion, Type.SparseUnion]; break;
            default: typeIds = [TType]; break;
        }
        typeIds.forEach((TType) => {
            type T = DType[typeof TType];
            const VectorCtor = getVectorConstructor(TType);
            VectorCtor.prototype['get'] = partial1(getVisitor.getVisitFn<T>(<any> TType));
            VectorCtor.prototype['indexOf'] = partial2(indexOfVisitor.getVisitFn<T>(<any> TType));
            VectorCtor.prototype[Symbol.iterator] = partial0(iteratorVisitor.getVisitFn<T>(<any> TType));
        });
    });

function partial0<T extends DataType, V extends VType[T['TType']]>(visit: (node: V) => any) {
    return function(this: V) { return visit(this); };
}
function partial1<T extends DataType, V extends VType[T['TType']]>(visit: (node: V, a: any) => any) {
    return function(this: V, a: any) { return visit(this, a); };
}
function partial2<T extends DataType, V extends VType[T['TType']]>(visit: (node: V, a: any, b: any) => any) {
    return function(this: V, a: any, b: any) { return visit(this, a, b); };
}
// function partial3<T extends DataType, V extends VTypes[T['TType']]>(visit: (node: VType[T['TType']], a: any, b: any, c: any) => any) {
//     return function(this: V, a: any, b: any, c: any) { return visit(this, a, b, c); };
// }

export type VType<T extends DataType = any> = {
    [Type.NONE]                 : Vector;
    [Type.Null]                 : NullVector;
    [Type.Bool]                 : BoolVector;
    [Type.Int]                  : IntVector;
    [Type.Int8]                 : Int8Vector;
    [Type.Int16]                : Int16Vector;
    [Type.Int32]                : Int32Vector;
    [Type.Int64]                : Int64Vector;
    [Type.Uint8]                : Uint8Vector;
    [Type.Uint16]               : Uint16Vector;
    [Type.Uint32]               : Uint32Vector;
    [Type.Uint64]               : Uint64Vector;
    [Type.Float]                : FloatVector;
    [Type.Float16]              : Float16Vector;
    [Type.Float32]              : Float32Vector;
    [Type.Float64]              : Float64Vector;
    [Type.Utf8]                 : Utf8Vector;
    [Type.Binary]               : BinaryVector;
    [Type.FixedSizeBinary]      : FixedSizeBinaryVector;
    [Type.Date]                 : DateVector;
    [Type.DateDay]              : DateDayVector;
    [Type.DateMillisecond]      : DateMillisecondVector;
    [Type.Timestamp]            : TimestampVector;
    [Type.TimestampSecond]      : TimestampSecondVector;
    [Type.TimestampMillisecond] : TimestampMillisecondVector;
    [Type.TimestampMicrosecond] : TimestampMicrosecondVector;
    [Type.TimestampNanosecond]  : TimestampNanosecondVector;
    [Type.Time]                 : TimeVector;
    [Type.TimeSecond]           : TimeSecondVector;
    [Type.TimeMillisecond]      : TimeMillisecondVector;
    [Type.TimeMicrosecond]      : TimeMicrosecondVector;
    [Type.TimeNanosecond]       : TimeNanosecondVector;
    [Type.Decimal]              : DecimalVector;
    [Type.List]                 : ListVector<T>;
    [Type.Struct]               : StructVector;
    [Type.Union]                : UnionVector;
    [Type.DenseUnion]           : DenseUnionVector;
    [Type.SparseUnion]          : SparseUnionVector;
    [Type.Dictionary]           : DictionaryVector<T>;
    [Type.Interval]             : IntervalVector;
    [Type.IntervalDayTime]      : IntervalDayTimeVector;
    [Type.IntervalYearMonth]    : IntervalYearMonthVector;
    [Type.FixedSizeList]        : FixedSizeListVector<T>;
    [Type.Map]                  : MapVector;
};

export type VCtor = {
    [Type.NONE]                 : (new                           (                                  ...args: any[]) => Vector);
    [Type.Null]                 : (new                           (data: Data<Null>,                 ...args: any[]) => NullVector);
    [Type.Bool]                 : (new                           (data: Data<Bool>,                 ...args: any[]) => BoolVector);
    [Type.Int]                  : (new                           (data: Data<Int>,                  ...args: any[]) => IntVector);
    [Type.Int8]                 : (new                           (data: Data<Int8>,                 ...args: any[]) => Int8Vector);
    [Type.Int16]                : (new                           (data: Data<Int16>,                ...args: any[]) => Int16Vector);
    [Type.Int32]                : (new                           (data: Data<Int32>,                ...args: any[]) => Int32Vector);
    [Type.Int64]                : (new                           (data: Data<Int64>,                ...args: any[]) => Int64Vector);
    [Type.Uint8]                : (new                           (data: Data<Uint8>,                ...args: any[]) => Uint8Vector);
    [Type.Uint16]               : (new                           (data: Data<Uint16>,               ...args: any[]) => Uint16Vector);
    [Type.Uint32]               : (new                           (data: Data<Uint32>,               ...args: any[]) => Uint32Vector);
    [Type.Uint64]               : (new                           (data: Data<Uint64>,               ...args: any[]) => Uint64Vector);
    [Type.Float]                : (new                           (data: Data<Float>,                ...args: any[]) => FloatVector);
    [Type.Float16]              : (new                           (data: Data<Float16>,              ...args: any[]) => Float16Vector);
    [Type.Float32]              : (new                           (data: Data<Float32>,              ...args: any[]) => Float32Vector);
    [Type.Float64]              : (new                           (data: Data<Float64>,              ...args: any[]) => Float64Vector);
    [Type.Utf8]                 : (new                           (data: Data<Utf8>,                 ...args: any[]) => Utf8Vector);
    [Type.Binary]               : (new                           (data: Data<Binary>,               ...args: any[]) => BinaryVector);
    [Type.FixedSizeBinary]      : (new                           (data: Data<FixedSizeBinary>,      ...args: any[]) => FixedSizeBinaryVector);
    [Type.Date]                 : (new                           (data: Data<Date_>,                ...args: any[]) => DateVector);
    [Type.DateDay]              : (new                           (data: Data<DateDay>,              ...args: any[]) => DateDayVector);
    [Type.DateMillisecond]      : (new                           (data: Data<DateMillisecond>,      ...args: any[]) => DateMillisecondVector);
    [Type.Timestamp]            : (new                           (data: Data<Timestamp>,            ...args: any[]) => TimestampVector);
    [Type.TimestampSecond]      : (new                           (data: Data<TimestampSecond>,      ...args: any[]) => TimestampSecondVector);
    [Type.TimestampMillisecond] : (new                           (data: Data<TimestampMillisecond>, ...args: any[]) => TimestampMillisecondVector);
    [Type.TimestampMicrosecond] : (new                           (data: Data<TimestampMicrosecond>, ...args: any[]) => TimestampMicrosecondVector);
    [Type.TimestampNanosecond]  : (new                           (data: Data<TimestampNanosecond>,  ...args: any[]) => TimestampNanosecondVector);
    [Type.Time]                 : (new                           (data: Data<Time>,                 ...args: any[]) => TimeVector);
    [Type.TimeSecond]           : (new                           (data: Data<TimeSecond>,           ...args: any[]) => TimeSecondVector);
    [Type.TimeMillisecond]      : (new                           (data: Data<TimeMillisecond>,      ...args: any[]) => TimeMillisecondVector);
    [Type.TimeMicrosecond]      : (new                           (data: Data<TimeMicrosecond>,      ...args: any[]) => TimeMicrosecondVector);
    [Type.TimeNanosecond]       : (new                           (data: Data<TimeNanosecond>,       ...args: any[]) => TimeNanosecondVector);
    [Type.Decimal]              : (new                           (data: Data<Decimal>,              ...args: any[]) => DecimalVector);
    [Type.List]                 : (new <T extends DataType = any>(data: Data<List<T>>,              ...args: any[]) => ListVector<T>);
    [Type.Struct]               : (new                           (data: Data<Struct>,               ...args: any[]) => StructVector);
    [Type.Union]                : (new                           (data: Data<Union>,                ...args: any[]) => UnionVector);
    [Type.DenseUnion]           : (new                           (data: Data<DenseUnion>,           ...args: any[]) => DenseUnionVector);
    [Type.SparseUnion]          : (new                           (data: Data<SparseUnion>,          ...args: any[]) => SparseUnionVector);
    [Type.Dictionary]           : (new <T extends DataType = any>(dictionary: Vector<T>, indices: Vector<Int>     ) => DictionaryVector<T>);
    [Type.Interval]             : (new                           (data: Data<Interval>,             ...args: any[]) => IntervalVector);
    [Type.IntervalDayTime]      : (new                           (data: Data<IntervalDayTime>,      ...args: any[]) => IntervalDayTimeVector);
    [Type.IntervalYearMonth]    : (new                           (data: Data<IntervalYearMonth>,    ...args: any[]) => IntervalYearMonthVector);
    [Type.FixedSizeList]        : (new <T extends DataType = any>(data: Data<FixedSizeList<T>>,     ...args: any[]) => FixedSizeListVector<T>);
    [Type.Map]                  : (new                           (data: Data<Map_>,                 ...args: any[]) => MapVector);
};
