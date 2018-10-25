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
import { Field } from './schema';
import { clampRange } from './util/vector';
import { Row } from './type';
import { Vector, VectorCtorArgs } from './interfaces';
import { instance as getVisitor } from './visitor/get';
import { instance as indexOfVisitor } from './visitor/indexof';
import { instance as toArrayVisitor } from './visitor/toarray';
import { instance as iteratorVisitor } from './visitor/iterator';
import { instance as byteWidthVisitor } from './visitor/bytewidth';
import { instance as getVectorConstructor } from './visitor/vectorctor';
import {
    IterableArrayLike,
    DataType, Dictionary,
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

interface ArrowVector<T extends DataType = any> {

    getByteWidth(): number;
    isValid(index: number): boolean;
    get(index: number): T['TValue'] | null;
    indexOf(element: T['TValue'] | null, index?: number): number;

    toJSON(): any;
    toArray(): IterableArrayLike<T['TValue'] | null>;
    [Symbol.iterator](): IterableIterator<T['TValue'] | null>;
}

class ArrowVector<T extends DataType = any> {

    static new <T extends DataType>(data: Data<T>, ...args: VectorCtorArgs<Vector<T>>): Vector<T> {
        return new ArrowVector<T>(data, ...args) as Vector<T>;
    }

    public readonly data: Data<T>;
    public readonly stride: number;
    public readonly numChildren: number;
    protected _children: Vector[] | void;

    constructor(data: Data<T>, children?: Vector[], stride?: number) {
        const VectorCtor = getVectorConstructor.getVisitFn(data.type)();
        // Return the correct Vector subclass based on the Arrow Type
        if (!(this instanceof VectorCtor)) {
            return Reflect.construct(ArrowVector, arguments, VectorCtor);
        }
        this._children = children;
        this.bindDataAccessors(this.data = data);
        this.numChildren = data.childData.length;
        this.stride = Math.floor(Math.max(stride || 1, 1));
    }

    // @ts-ignore
    protected bindDataAccessors(data: Data<T>) {}

    public get type() { return this.data.type; }
    public get length() { return this.data.length; }
    public get offset() { return this.data.offset; }
    public get nullCount() { return this.data.nullCount; }
    public get VectorName() { return this.constructor.name; }
    public get TType(): T['TType'] { return this.data.TType; }
    public get TArray(): T['TArray'] { return this.data.TArray; }
    public get TValue(): T['TValue'] { return this.data.TValue; }
    public get ArrayType(): T['ArrayType'] { return this.data.ArrayType; }

    public get values() { return this.data.values; }
    public get typeIds() { return this.data.typeIds; }
    public get nullBitmap() { return this.data.nullBitmap; }
    public get valueOffsets() { return this.data.valueOffsets; }

    public get [Symbol.toStringTag]() { return `${this.VectorName}<${this.type[Symbol.toStringTag]}>`; }

    public clone<R extends DataType = T>(data: Data<R>, children = this._children, stride = this.stride) {
        return ArrowVector.new<R>(data, children, stride);
    }

    public isValid(index: number): boolean {
        if (this.nullCount > 0) {
            const idx = this.offset + index;
            const val = this.nullBitmap[idx >> 3];
            const mask = (val & (1 << (idx % 8)));
            return mask !== 0;
        }
        return true;
    }

    public getChildAt<R extends DataType = any>(index: number): Vector<R> | null {
        return index < 0 || index >= this.numChildren ? null : (
            (this._children || (this._children = []))[index] ||
            (this._children[index] = ArrowVector.new<R>(this.data.childData[index] as Data<R>))
        ) as Vector<R>;
    }

    // @ts-ignore
    public toJSON(): any {}

    public slice(begin?: number, end?: number): this {
        // Adjust args similar to Array.prototype.slice. Normalize begin/end to
        // clamp between 0 and length, and wrap around on negative indices, e.g.
        // slice(-1, 5) or slice(5, -1)
        return clampRange(this, begin, end, (x, y, z) => x.clone(x.data.slice(y, z))) as any;
    }
}

export { ArrowVector as Vector };

export class NullVector                                       extends ArrowVector<Null> {}

export class IntVector<T extends Int = any>                   extends ArrowVector<T> {}
export class Int8Vector                                       extends IntVector<Int8> {}
export class Int16Vector                                      extends IntVector<Int16> {}
export class Int32Vector                                      extends IntVector<Int32> {}
export class Int64Vector                                      extends IntVector<Int64> {}
export class Uint8Vector                                      extends IntVector<Uint8> {}
export class Uint16Vector                                     extends IntVector<Uint16> {}
export class Uint32Vector                                     extends IntVector<Uint32> {}
export class Uint64Vector                                     extends IntVector<Uint64> {}

export class FloatVector<T extends Float = any>               extends ArrowVector<T> {}
export class Float16Vector                                    extends FloatVector<Float16> {}
export class Float32Vector                                    extends FloatVector<Float32> {}
export class Float64Vector                                    extends FloatVector<Float64> {}

export class BoolVector                                       extends ArrowVector<Bool> {}
export class DecimalVector                                    extends ArrowVector<Decimal> {}

export class DateVector<T extends Date_ = Date_>              extends ArrowVector<T> {}
export class DateDayVector                                    extends DateVector<DateDay> {}
export class DateMillisecondVector                            extends DateVector<DateMillisecond> {}

export class TimeVector<T extends Time = Time>                extends ArrowVector<T> {}
export class TimeSecondVector                                 extends TimeVector<TimeSecond> {}
export class TimeMillisecondVector                            extends TimeVector<TimeMillisecond> {}
export class TimeMicrosecondVector                            extends TimeVector<TimeMicrosecond> {}
export class TimeNanosecondVector                             extends TimeVector<TimeNanosecond> {}

export class TimestampVector<T extends Timestamp = Timestamp> extends ArrowVector<T> {}
export class TimestampSecondVector                            extends TimestampVector<TimestampSecond> {}
export class TimestampMillisecondVector                       extends TimestampVector<TimestampMillisecond> {}
export class TimestampMicrosecondVector                       extends TimestampVector<TimestampMicrosecond> {}
export class TimestampNanosecondVector                        extends TimestampVector<TimestampNanosecond> {}

export class IntervalVector<T extends Interval = Interval>    extends ArrowVector<T> {}
export class IntervalDayTimeVector                            extends IntervalVector<IntervalDayTime> {}
export class IntervalYearMonthVector                          extends IntervalVector<IntervalYearMonth> {}

export class BinaryVector extends ArrowVector<Binary> {
    public asUtf8() {
        return ArrowVector.new(this.data.clone(new Utf8()));
    }
}

export class Utf8Vector extends ArrowVector<Utf8> {
    public asBinary() {
        return ArrowVector.new(this.data.clone(new Binary()));
    }
}

export class ListVector<T extends DataType = any> extends ArrowVector<List<T>> {}

export class StructVector<T extends { [key: string]: DataType } = any> extends ArrowVector<Struct<T>> {
    public rowProxy: RowProxy<T> = RowProxy.new<T>(this.type.children || []);
    public asMap(keysSorted: boolean = false) {
        return ArrowVector.new(this.data.clone(new Map_(this.type.children, keysSorted)));
    }
}

export class UnionVector<T extends Union = Union> extends ArrowVector<T> {
    public get typeIdToChildIndex() { return this.type.typeIdToChildIndex; }
}

export class DenseUnionVector extends UnionVector<DenseUnion> {
    public get valueOffsets() { return this.data.valueOffsets!; }
}

export class SparseUnionVector extends UnionVector<SparseUnion> {}

export class FixedSizeBinaryVector extends ArrowVector<FixedSizeBinary> {
    constructor(data: Data<FixedSizeBinary>) {
        super(data, void 0, data.type.byteWidth);
    }
}

export class FixedSizeListVector<T extends DataType = any> extends ArrowVector<FixedSizeList<T>> {
    constructor(data: Data<FixedSizeList<T>>) {
        super(data, void 0, data.type.listSize);
    }
}

export class MapVector<T extends { [key: string]: DataType } = any> extends ArrowVector<Map_<T>> {
    public rowProxy: RowProxy<T> = RowProxy.new<T>(this.type.children || []);
    public asStruct() {
        return ArrowVector.new(this.data.clone(new Struct(this.type.children)));
    }
}

export class DictionaryVector<T extends DataType = any> extends ArrowVector<Dictionary<T>> {
    public readonly indices: Vector<Int>;
    public readonly dictionary: Vector<T>;
    constructor(data: Data<Dictionary<T>>) {
        super(data, void 0, 1);
        this.dictionary = this.type.dictionary;
        this.indices = new ArrowVector(data.clone(this.type.indices));
    }
    public getKey(index: number) { return this.indices.get(index); }
    public getValue(key: number) { return this.dictionary.get(key); }
    public reverseLookup(value: T) { return this.dictionary.indexOf(value); }
}

// Perf: bind and assign the operator Visitor methods to each of the Vector subclasses for each Type
(Object.keys(Type) as any[])
    .filter((TType) => TType !== Type.NONE && TType !== Type[Type.NONE])
    .filter((T: any): T is Type => typeof Type[T] === 'number')
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
            default:             typeIds = [TType]; break;
        }
        typeIds.forEach((TType) => {
            const VectorCtor = getVectorConstructor.visit(TType);
            VectorCtor.prototype['get'] = partial1(getVisitor.getVisitFn(<any> TType));
            VectorCtor.prototype['indexOf'] = partial2(indexOfVisitor.getVisitFn(<any> TType));
            VectorCtor.prototype['toArray'] = partial0(toArrayVisitor.getVisitFn(<any> TType));
            VectorCtor.prototype['getByteWidth'] = partial0(byteWidthVisitor.getVisitFn(<any> TType));
            VectorCtor.prototype[Symbol.iterator] = partial0(iteratorVisitor.getVisitFn(<any> TType));
        });
    });

function partial0<T extends DataType, V extends Vector<T>>(visit: (node: V) => any) {
    return function(this: V) { return visit(this); };
}

function partial1<T extends DataType, V extends Vector<T>>(visit: (node: V, a: any) => any) {
    return function(this: V, a: any) { return visit(this, a); };
}

function partial2<T extends DataType, V extends Vector<T>>(visit: (node: V, a: any, b: any) => any) {
    return function(this: V, a: any, b: any) { return visit(this, a, b); };
}

// function partial3<T extends DataType, V extends VTypes[T['TType']]>(visit: (node: VType[T['TType']], a: any, b: any, c: any) => any) {
//     return function(this: V, a: any, b: any, c: any) { return visit(this, a, b, c); };
// }

const columnDescriptor = { writable: false, enumerable: true, configurable: false, get: () => {} };
const rowIndexDescriptor = { writable: false, enumerable: true, configurable: true, value: null as any };
const rowParentDescriptor = { writable: false, enumerable: true, configurable: false, value: null as any };
const row = { parent: rowParentDescriptor, rowIndex: rowIndexDescriptor };

export class RowProxy<T extends { [key: string]: DataType }> implements Iterable<T[keyof T]['TValue']> {
    static new<T extends { [key: string]: DataType }>(schemaOrFields: T | Field[]): Row<T> & RowProxy<T> {
        let schema: T, fields: Field[];
        if (Array.isArray(schemaOrFields)) {
            fields = schemaOrFields;
        } else {
            schema = schemaOrFields;
            fields = Object.keys(schema).map((x) => new Field(x, schema[x]));
        }
        return new RowProxy<T>(fields) as Row<T> & RowProxy<T>;
    }
    // @ts-ignore
    private parent: TParent;
    // @ts-ignore
    private rowIndex: number;
    public readonly length: number;
    private constructor(fields: Field[]) {
        this.length = fields.length;
        fields.forEach((field, columnIndex) => {
            columnDescriptor.get = this._bindGetter(columnIndex);
            Object.defineProperty(this, field.name, columnDescriptor);
            Object.defineProperty(this, columnIndex, columnDescriptor);
        });
    }
    *[Symbol.iterator](this: Row<T>) {
        for (let i = -1, n = this.length; ++i < n;) {
            yield this[i];
        }
    }
    private _bindGetter(colIndex: number) {
        return function (this: RowProxy<T>) {
            let child = this.parent.getChildAt(colIndex);
            return child ? child.get(this.rowIndex) : null;
        };
    }
    public get<K extends keyof T>(key: K) { return (this as any)[key] as T[K]['TValue']; }
    public bind<TParent extends MapVector<T> | StructVector<T>>(parent: TParent, rowIndex: number) {
        rowIndexDescriptor.value = rowIndex;
        rowParentDescriptor.value = parent;
        const bound = Object.create(this, row);
        rowIndexDescriptor.value = null;
        rowParentDescriptor.value = null;
        return bound as Row<T>;
    }
}

// export namespace constructors {
//     export type Vector                                        = (new (                                  ...args: any[]) => Vector);
//     export type NullVector                                    = (new (data: Data<Null>,                 ...args: any[]) => NullVector);
//     export type BoolVector                                    = (new (data: Data<Bool>,                 ...args: any[]) => BoolVector);
//     export type IntVector                                     = (new (data: Data<Int>,                  ...args: any[]) => IntVector);
//     export type Int8Vector                                    = (new (data: Data<Int8>,                 ...args: any[]) => Int8Vector);
//     export type Int16Vector                                   = (new (data: Data<Int16>,                ...args: any[]) => Int16Vector);
//     export type Int32Vector                                   = (new (data: Data<Int32>,                ...args: any[]) => Int32Vector);
//     export type Int64Vector                                   = (new (data: Data<Int64>,                ...args: any[]) => Int64Vector);
//     export type Uint8Vector                                   = (new (data: Data<Uint8>,                ...args: any[]) => Uint8Vector);
//     export type Uint16Vector                                  = (new (data: Data<Uint16>,               ...args: any[]) => Uint16Vector);
//     export type Uint32Vector                                  = (new (data: Data<Uint32>,               ...args: any[]) => Uint32Vector);
//     export type Uint64Vector                                  = (new (data: Data<Uint64>,               ...args: any[]) => Uint64Vector);
//     export type FloatVector                                   = (new (data: Data<Float>,                ...args: any[]) => FloatVector);
//     export type Float16Vector                                 = (new (data: Data<Float16>,              ...args: any[]) => Float16Vector);
//     export type Float32Vector                                 = (new (data: Data<Float32>,              ...args: any[]) => Float32Vector);
//     export type Float64Vector                                 = (new (data: Data<Float64>,              ...args: any[]) => Float64Vector);
//     export type Utf8Vector                                    = (new (data: Data<Utf8>,                 ...args: any[]) => Utf8Vector);
//     export type BinaryVector                                  = (new (data: Data<Binary>,               ...args: any[]) => BinaryVector);
//     export type FixedSizeBinaryVector                         = (new (data: Data<FixedSizeBinary>,      ...args: any[]) => FixedSizeBinaryVector);
//     export type DateVector                                    = (new (data: Data<Date_>,                ...args: any[]) => DateVector);
//     export type DateDayVector                                 = (new (data: Data<DateDay>,              ...args: any[]) => DateDayVector);
//     export type DateMillisecondVector                         = (new (data: Data<DateMillisecond>,      ...args: any[]) => DateMillisecondVector);
//     export type TimestampVector                               = (new (data: Data<Timestamp>,            ...args: any[]) => TimestampVector);
//     export type TimestampSecondVector                         = (new (data: Data<TimestampSecond>,      ...args: any[]) => TimestampSecondVector);
//     export type TimestampMillisecondVector                    = (new (data: Data<TimestampMillisecond>, ...args: any[]) => TimestampMillisecondVector);
//     export type TimestampMicrosecondVector                    = (new (data: Data<TimestampMicrosecond>, ...args: any[]) => TimestampMicrosecondVector);
//     export type TimestampNanosecondVector                     = (new (data: Data<TimestampNanosecond>,  ...args: any[]) => TimestampNanosecondVector);
//     export type TimeVector                                    = (new (data: Data<Time>,                 ...args: any[]) => TimeVector);
//     export type TimeSecondVector                              = (new (data: Data<TimeSecond>,           ...args: any[]) => TimeSecondVector);
//     export type TimeMillisecondVector                         = (new (data: Data<TimeMillisecond>,      ...args: any[]) => TimeMillisecondVector);
//     export type TimeMicrosecondVector                         = (new (data: Data<TimeMicrosecond>,      ...args: any[]) => TimeMicrosecondVector);
//     export type TimeNanosecondVector                          = (new (data: Data<TimeNanosecond>,       ...args: any[]) => TimeNanosecondVector);
//     export type DecimalVector                                 = (new (data: Data<Decimal>,              ...args: any[]) => DecimalVector);
//     export type UnionVector                                   = (new (data: Data<Union>,                ...args: any[]) => UnionVector);
//     export type DenseUnionVector                              = (new (data: Data<DenseUnion>,           ...args: any[]) => DenseUnionVector);
//     export type SparseUnionVector                             = (new (data: Data<SparseUnion>,          ...args: any[]) => SparseUnionVector);
//     export type IntervalVector                                = (new (data: Data<Interval>,             ...args: any[]) => IntervalVector);
//     export type IntervalDayTimeVector                         = (new (data: Data<IntervalDayTime>,      ...args: any[]) => IntervalDayTimeVector);
//     export type IntervalYearMonthVector                       = (new (data: Data<IntervalYearMonth>,    ...args: any[]) => IntervalYearMonthVector);
//     export type ListVector<T extends DataType = any>          = (new (data: Data<List<T>>,              ...args: any[]) => ListVector<T>);
//     export type StructVector<T extends DataTypes = any>       = (new (data: Data<Struct<T>>,            ...args: any[]) => StructVector<T>);
//     export type FixedSizeListVector<T extends DataType = any> = (new (data: Data<FixedSizeList<T>>,     ...args: any[]) => FixedSizeListVector<T>);
//     export type MapVector<T extends DataTypes = any>          = (new (data: Data<Map_<T>>,              ...args: any[]) => MapVector<T>);
//     export type DictionaryVector<T extends DataType = any>    = (new (data: Data<Dictionary<T>>,        ...args: any[]) => DictionaryVector<T>);
// };
