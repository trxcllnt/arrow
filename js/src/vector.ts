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
import { setBool } from './util/bit';
import * as IntUtil from './util/int';
import { Type, DateUnit } from './enum';
import { clampRange } from './util/vector';
import { Vector as V, VectorCtorArgs } from './interfaces';
import { instance as getVisitor } from './visitor/get';
import { instance as indexOfVisitor } from './visitor/indexof';
import { instance as toArrayVisitor } from './visitor/toarray';
import { instance as iteratorVisitor } from './visitor/iterator';
import { instance as byteWidthVisitor } from './visitor/bytewidth';
import { instance as setVisitor, encodeUtf8 } from './visitor/set';
import { instance as getVectorConstructor } from './visitor/vectorctor';
import {
    DataType, Dictionary,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Map_, Struct, Union,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp,

    DenseUnion, SparseUnion,
    DateDay, DateMillisecond,
    IntervalDayTime, IntervalYearMonth,
    TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Float16, Float32, Float64, TKeys,
} from './type';

export abstract class Vector<T extends DataType = any> implements Iterable<T['TValue'] | null> {

    static new <T extends DataType>(data: Data<T>, ...args: VectorCtorArgs<V<T>>): V<T> {
        return new (getVectorConstructor.getVisitFn(data.type)())(data, ...args) as V<T>;
    }

    // @ts-ignore
    protected bindDataAccessors(data: Data<T>) {
        if (this.nullCount > 0) {
            this['get'] && (this['get'] = wrapNullable1(this['get']));
            this['set'] && (this['set'] = wrapNullableSet(this['set']));
        }
    }

    public abstract readonly type: T;
    public abstract readonly data: Data<T>;
    public abstract readonly length: number;
    public abstract readonly stride: number;
    public abstract readonly nullCount: number;
    public abstract readonly numChildren: number;

    public abstract readonly TType: T['TType'];
    public abstract readonly TArray: T['TArray'];
    public abstract readonly TValue: T['TValue'];
    public abstract readonly ArrayType: T['ArrayType'];

    public abstract isValid(index: number): boolean;
    public abstract get(index: number): T['TValue'] | null;
    public abstract set(index: number, value: T['TValue'] | null): void;
    public abstract indexOf(value: T['TValue'] | null, fromIndex?: number): number;

    public abstract toArray(): T['TArray'];
    public abstract [Symbol.iterator](): IterableIterator<T['TValue'] | null>;
    public abstract slice(begin?: number, end?: number): Vector<T>;
    public abstract concat(this: Vector<T>, ...others: Vector<T>[]): Vector<T>;

    public abstract getChildAt<R extends DataType = any>(index: number): Vector<R> | null;
}

import { Row, ChunkedVector } from './column';
import { packBools } from './util/bit';

export class BaseVector<T extends DataType = any> extends Vector<T> {

    // @ts-ignore
    public readonly data: Data<T>;
    public readonly stride: number = 1;
    public readonly numChildren: number = 0;
    protected _children?: Vector[];

    constructor(data: Data<T>, children?: Vector[], stride?: number) {
        super();
        const VectorCtor = getVectorConstructor.getVisitFn(data.type)();
        // Return the correct Vector subclass based on the Arrow Type
        if (VectorCtor && !(this instanceof VectorCtor)) {
            return Reflect.construct(BaseVector, arguments, VectorCtor);
        }
        this._children = children;
        this.bindDataAccessors(this.data = data);
        this.numChildren = data.childData.length;
        this.stride = Math.floor(Math.max(stride || 1, 1));
    }

    // @ts-ignore
    protected bindDataAccessors(data: Data<T>) {
        const type = this.type;
        this['get'] = getVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
        this['set'] = setVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
        this['indexOf'] = indexOfVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
        this['toArray'] = toArrayVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
        this[Symbol.iterator] = iteratorVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
        super.bindDataAccessors(data);
    }

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
        return Vector.new<R>(data, children, stride);
    }

    public concat(...others: Vector<T>[]): Vector<T> {
        return ChunkedVector.concat<T>(this, ...others) as Vector<T>;
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
            (this._children[index] = Vector.new<R>(this.data.childData[index] as Data<R>))
        ) as Vector<R>;
    }

    // @ts-ignore
    public toJSON(): any {}

    public slice(begin?: number, end?: number): this {
        // Adjust args similar to Array.prototype.slice. Normalize begin/end to
        // clamp between 0 and length, and wrap around on negative indices, e.g.
        // slice(-1, 5) or slice(5, -1)
        const { stride } = this;
        return clampRange(this, begin, end, (x, y, z) => x.clone(x.data.slice(y * stride, (z - y) * stride))) as any;
    }

    //
    // We provide the following method implementations for code navigability purposes only.
    // They're overridden at runtime with the specific Visitor implementation for each type,
    // short-circuiting the usual Visitor traversal and reducing intermediate lookups and calls.
    // This comment is here to remind you to not set breakpoints in these fn bodies, or to inform
    // you why the breakpoints you have already set are not being triggered. Have a great day!
    //
    public get(index: number): T['TValue'] | null {
        return getVisitor.visit(this, index);
    }
    public set(index: number, value: T['TValue'] | null): void {
        return setVisitor.visit(this, index, value);
    }
    public indexOf(value: T['TValue'] | null, fromIndex?: number): number {
        return indexOfVisitor.visit(this, value, fromIndex);
    }
    public toArray(): T['TArray'] {
        return toArrayVisitor.visit(this);
    }
    public getByteWidth(): number {
        return byteWidthVisitor.visit(this);
    }
    public [Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        return iteratorVisitor.visit(this);
    }
}

export class NullVector                                       extends BaseVector<Null> {}

export class IntVector<T extends Int = Int>                   extends BaseVector<T> {
    public static from(data: Int8Array): V<Int8>;
    public static from(data: Int16Array): V<Int16>;
    public static from(data: Int32Array): V<Int32>;
    public static from(data: Uint8Array): V<Uint8>;
    public static from(data: Uint16Array): V<Uint16>;
    public static from(data: Uint32Array): V<Uint32>;
    public static from(data: Int32Array, is64: true): V<Int64>;
    public static from(data: Uint32Array, is64: true): V<Uint64>;
    public static from(data: any, is64?: boolean) {
        if (is64 === true) {
            return data instanceof Int32Array
                ? Vector.new(Data.Int(new Int64(), 0, data.length, 0, null, data))
                : Vector.new(Data.Int(new Uint64(), 0, data.length, 0, null, data));
        }
        switch (data.constructor) {
            case Int8Array: return Vector.new(Data.Int(new Int8(), 0, data.length, 0, null, data));
            case Int16Array: return Vector.new(Data.Int(new Int16(), 0, data.length, 0, null, data));
            case Int32Array: return Vector.new(Data.Int(new Int32(), 0, data.length, 0, null, data));
            case Uint8Array: return Vector.new(Data.Int(new Uint8(), 0, data.length, 0, null, data));
            case Uint16Array: return Vector.new(Data.Int(new Uint16(), 0, data.length, 0, null, data));
            case Uint32Array: return Vector.new(Data.Int(new Uint32(), 0, data.length, 0, null, data));
        }
        throw new TypeError('Unrecognized Int data');
    }
    constructor(data: Data<T>) {
        super(data, undefined, data.type.bitWidth <= 32 ? 1 : 2);
    }
}

export class Int8Vector                                       extends IntVector<Int8> {}
export class Int16Vector                                      extends IntVector<Int16> {}
export class Int32Vector                                      extends IntVector<Int32> {}
export class Int64Vector                                      extends IntVector<Int64> {}
export class Uint8Vector                                      extends IntVector<Uint8> {}
export class Uint16Vector                                     extends IntVector<Uint16> {}
export class Uint32Vector                                     extends IntVector<Uint32> {}
export class Uint64Vector                                     extends IntVector<Uint64> {}

export class FloatVector<T extends Float = any>               extends BaseVector<T> {
    public static from(data: Uint16Array): V<Float16>;
    public static from(data: Float32Array): V<Float32>;
    public static from(data: Float64Array): V<Float64>;
    public static from(data: any) {
        switch (data.constructor) {
            case Uint16Array: return Vector.new(Data.Float(new Float16(), 0, data.length, 0, null, data));
            case Float32Array: return Vector.new(Data.Float(new Float32(), 0, data.length, 0, null, data));
            case Float64Array: return Vector.new(Data.Float(new Float64(), 0, data.length, 0, null, data));
        }
        throw new TypeError('Unrecognized Float data');
    }
}
export class Float16Vector                                    extends FloatVector<Float16> {}
export class Float32Vector                                    extends FloatVector<Float32> {}
export class Float64Vector                                    extends FloatVector<Float64> {}

export class BoolVector                                       extends BaseVector<Bool> {
    public static from(data: Iterable<boolean>) {
        let length = 0, bitmap = packBools(function*() {
            for (let x of data) { length++; yield x; }
        }());
        return Vector.new(Data.Bool(new Bool(), 0, length, 0, null, bitmap));
    }
}
export class DecimalVector                                    extends BaseVector<Decimal> {
    constructor(data: Data<Decimal>) {
        super(data, undefined, 4);
    }
}

export class DateVector<T extends Date_ = Date_>              extends BaseVector<T> {
    static from<T extends Date_ = DateMillisecond>(data: Date[], unit: T['unit'] = DateUnit.MILLISECOND) {
        switch (unit) {
            case DateUnit.DAY: {
                const values = Int32Array.from(data.map((d) => d.valueOf() / 86400000));
                return Vector.new(Data.Date(new DateDay(), 0, data.length, 0, null, values));
            }
            case DateUnit.MILLISECOND: {
                const values = IntUtil.Int64.convertArray(data.map((d) => d.valueOf()));
                return Vector.new(Data.Date(new DateMillisecond(), 0, data.length, 0, null, values));
            }
        }
        throw new TypeError(`Unrecognized date unit "${DateUnit[unit]}"`);
    }
    constructor(data: Data<T>) {
        super(data, undefined, data.type.unit + 1);
    }
}
export class DateDayVector                                    extends DateVector<DateDay> {}
export class DateMillisecondVector                            extends DateVector<DateMillisecond> {}

export class TimeVector<T extends Time = Time>                extends BaseVector<T> {
    constructor(data: Data<T>) {
        super(data, undefined, data.type.bitWidth <= 32 ? 1 : 2);
    }
}
export class TimeSecondVector                                 extends TimeVector<TimeSecond> {}
export class TimeMillisecondVector                            extends TimeVector<TimeMillisecond> {}
export class TimeMicrosecondVector                            extends TimeVector<TimeMicrosecond> {}
export class TimeNanosecondVector                             extends TimeVector<TimeNanosecond> {}

export class TimestampVector<T extends Timestamp = Timestamp> extends BaseVector<T> {
    constructor(data: Data<T>) {
        super(data, undefined, 2);
    }
}
export class TimestampSecondVector                            extends TimestampVector<TimestampSecond> {}
export class TimestampMillisecondVector                       extends TimestampVector<TimestampMillisecond> {}
export class TimestampMicrosecondVector                       extends TimestampVector<TimestampMicrosecond> {}
export class TimestampNanosecondVector                        extends TimestampVector<TimestampNanosecond> {}

export class IntervalVector<T extends Interval = Interval>    extends BaseVector<T> {
    constructor(data: Data<T>) {
        super(data, undefined, data.type.unit + 1);
    }
}
export class IntervalDayTimeVector                            extends IntervalVector<IntervalDayTime> {}
export class IntervalYearMonthVector                          extends IntervalVector<IntervalYearMonth> {}

export class BinaryVector extends BaseVector<Binary> {
    public asUtf8() {
        return Vector.new(this.data.clone(new Utf8()));
    }
}

export class Utf8Vector extends BaseVector<Utf8> {
    public static from(values: string[]) {
        const length = values.length;
        const data = encodeUtf8(values.join(''));
        const offsets = values.reduce((offsets, str, idx) => (
            (!(offsets[idx + 1] = offsets[idx] + str.length) || true) && offsets
        ), new Uint32Array(values.length + 1));
        return Vector.new(Data.Utf8(new Utf8(), 0, length, 0, null, offsets, data));
    }
    public asBinary() {
        return Vector.new(this.data.clone(new Binary()));
    }
}

export class ListVector<T extends DataType = any> extends BaseVector<List<T>> {}

export class StructVector<T extends { [key: string]: DataType } = any> extends BaseVector<Struct<T>> {
    public rowProxy: Row<T> = Row.new<T>(this.type.children || [], false);
    public asMap(keysSorted: boolean = false) {
        return Vector.new(this.data.clone(new Map_(this.type.children, keysSorted)));
    }
}

export class UnionVector<T extends Union = Union> extends BaseVector<T> {
    public get typeIdToChildIndex() { return this.type.typeIdToChildIndex; }
}

export class DenseUnionVector extends UnionVector<DenseUnion> {
    public get valueOffsets() { return this.data.valueOffsets!; }
}

export class SparseUnionVector extends UnionVector<SparseUnion> {}

export class FixedSizeBinaryVector extends BaseVector<FixedSizeBinary> {
    constructor(data: Data<FixedSizeBinary>) {
        super(data, void 0, data.type.byteWidth);
    }
}

export class FixedSizeListVector<T extends DataType = any> extends BaseVector<FixedSizeList<T>> {
    constructor(data: Data<FixedSizeList<T>>) {
        super(data, void 0, data.type.listSize);
    }
}

export class MapVector<T extends { [key: string]: DataType } = any> extends BaseVector<Map_<T>> {
    public rowProxy: Row<T> = Row.new<T>(this.type.children || [], true);
    public asStruct() {
        return Vector.new(this.data.clone(new Struct(this.type.children)));
    }
}

export class DictionaryVector<T extends DataType = any, TKey extends TKeys = TKeys> extends BaseVector<Dictionary<T, TKey>> {
    public static from<T extends DataType<any>, TKey extends TKeys = TKeys>(
        values: Vector<T>, indices: TKey,
        keys: ArrayLike<number> | TKey['TArray']
    ) {
        const type = new Dictionary(values.type, indices, null, null, values);
        return Vector.new(Data.Dictionary(type, 0, keys.length, 0, null, keys));
    }
    public readonly indices: V<TKey>;
    constructor(data: Data<Dictionary<T, TKey>>) {
        super(data, void 0, 1);
        this.indices = Vector.new(data.clone(this.type.indices));
    }
    public get dictionary() { return this.type.dictionaryVector; }
    public getKey(index: number): TKey['TValue'] | null { return this.indices.get(index); }
    public getValue(key: number): T['TValue'] | null { return this.dictionary.get(key); }
    public isValid(index: number) { return this.indices.isValid(index); }
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
            VectorCtor.prototype['get'] = partial1(getVisitor.getVisitFn(TType));
            VectorCtor.prototype['set'] = partial2(setVisitor.getVisitFn(TType));
            VectorCtor.prototype['indexOf'] = partial2(indexOfVisitor.getVisitFn(TType));
            VectorCtor.prototype['toArray'] = partial0(toArrayVisitor.getVisitFn(TType));
            VectorCtor.prototype['getByteWidth'] = partial0(byteWidthVisitor.getVisitFn(TType));
            VectorCtor.prototype[Symbol.iterator] = partial0(iteratorVisitor.getVisitFn(TType));
        });
    });

function partial0<T>(visit: (node: T) => any) {
    return function(this: T) { return visit(this); };
}

function partial1<T>(visit: (node: T, a: any) => any) {
    return function(this: T, a: any) { return visit(this, a); };
}

function partial2<T>(visit: (node: T, a: any, b: any) => any) {
    return function(this: T, a: any, b: any) { return visit(this, a, b); };
}

function wrapNullable1<T extends DataType, V extends Vector<T>, F extends (i: number) => any>(fn: F): (...args: Parameters<F>) => ReturnType<F> {
    return function(this: V, i: number) { return this.isValid(i) ? fn.call(this, i) : null; };
}

function wrapNullableSet<T extends DataType, V extends BaseVector<T>, F extends (i: number, a: any) => void>(fn: F): (...args: Parameters<F>) => void {
    return function(this: V, i: number, a: any) {
        if (setBool(this.nullBitmap, this.offset + i, a != null)) {
            fn.call(this, i, a);
        }
    };
}
