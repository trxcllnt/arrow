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
import {
    Visitor,
    TypeVisitor, TypeNode,
    VectorVisitor, VectorNode,
    OperatorVisitor, OperatorNode
} from './visitor';

import {
    Type, DataType, Dictionary,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Map_, Struct, Union,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp,
} from './type';

import { IterableArrayLike, Precision, DateUnit, TimeUnit, IntervalUnit, UnionMode } from './type';
import { Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Float16, Float32, Float64 } from './type';

export interface Vector<T extends DataType = any> extends TypeNode, VectorNode, OperatorNode {
    toJSON(): any;
    isValid(index: number): boolean;
    get(index: number): T['TValue'] | null;
    toArray(): IterableArrayLike<T['TValue'] | null>;
    indexOf(element: T['TValue'] | null, index?: number): number;
    [Symbol.iterator](): IterableIterator<T['TValue'] | null>;
}

export class Vector<T extends DataType = any> {

    public readonly data: Data<T>;

    static new <T extends DataType>(data: Data<T>) {
        return new Vector(data) as VTypes[T['TType']];
    }

    constructor(data: Data<T>) {
        const VType = VCtors[data.TType];
        // Return the correct Vector subclass based on the Arrow Type
        if (!(this instanceof VType)) {
            return Reflect.construct(Vector, arguments, VType);
        }
        this.data = data;
    }

    public get type() { return this.data.type; }
    public get TType() { return this.data.TType; }
    public get length() { return this.data.length; }
    public get offset() { return this.data.offset; }
    public get stride() { return this.data.stride; }
    public get nullCount() { return this.data.nullCount; }
    public get nullBitmap() { return this.data.nullBitmap; }
    public get VectorName() { return this.constructor.name; }
    public get [Symbol.toStringTag]() { return `${this.VectorName}<${this.type[Symbol.toStringTag]}>`; }
    public clone<R extends DataType = T>(data: Data<R>): VTypes[R['TType']] { return Vector.new(data); }

    public slice(begin?: number, end?: number) {
        // Adjust args similar to Array.prototype.slice. Normalize begin/end to
        // clamp between 0 and length, and wrap around on negative indices, e.g.
        // slice(-1, 5) or slice(5, -1)
        let { length } = this;
        let total = length, from = (begin || 0);
        let to = (typeof end === 'number' ? end : total);
        if (to < 0) { to = total - (to * -1) % total; }
        if (from < 0) { from = total - (from * -1) % total; }
        if (to < from) { [from, to] = [to, from]; }
        total = !isFinite(total = (to - from)) || total < 0 ? 0 : total;
        return this.clone(this.data.slice(from, Math.min(total, length)));
    }

    public acceptTypeVisitor(visitor: TypeVisitor, ...args: any[]): any { return TypeVisitor.visitTypeInline(visitor, this.type, ...args); }
    public acceptVectorVisitor(visitor: VectorVisitor, ...args: any[]): any { return VectorVisitor.visitTypeInline(visitor, this, ...args); }
    public acceptOperatorVisitor(visitor: OperatorVisitor, ...args: any[]): any { return OperatorVisitor.visitTypeInline(visitor, this, ...args); }
}

export class NullVector extends Vector<Null> {}

export class IntVector extends Vector<Int> {}

export class FloatVector extends Vector<Float> {}

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

export class DateVector extends Vector<Date_> {}

export class TimeVector extends Vector<Time> {}

export class TimestampVector extends Vector<Timestamp> {}

export class IntervalVector extends Vector<Interval> {}

export class ListVector extends Vector<List> {}

export class StructVector extends Vector<Struct> {
    public asMap(keysSorted: boolean = false) {
        return Vector.new(this.data.clone(new Map_(keysSorted, this.type.children)));
    }
}

export class UnionVector extends Vector<Union> {}

export class FixedSizeBinaryVector extends Vector<FixedSizeBinary> {}

export class FixedSizeListVector extends Vector<FixedSizeList> {}

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

const getVisitor = new GetVisitor();
const indexOfVisitor = new IndexOfVisitor();
const iteratorVisitor = new IteratorVisitor();

const VCtors = Object.create(null) as VCtors;

VCtors[Type.Null]            = NullVector;
VCtors[Type.Int]             = IntVector;
VCtors[Type.Float]           = FloatVector;
VCtors[Type.Binary]          = BinaryVector;
VCtors[Type.Utf8]            = Utf8Vector;
VCtors[Type.Bool]            = BoolVector;
VCtors[Type.Decimal]         = DecimalVector;
VCtors[Type.Date]            = DateVector;
VCtors[Type.Time]            = TimeVector;
VCtors[Type.Timestamp]       = TimestampVector;
VCtors[Type.Interval]        = IntervalVector;
VCtors[Type.List]            = ListVector;
VCtors[Type.Struct]          = StructVector;
VCtors[Type.Union]           = UnionVector;
VCtors[Type.FixedSizeBinary] = FixedSizeBinaryVector;
VCtors[Type.FixedSizeList]   = FixedSizeListVector;
VCtors[Type.Map]             = MapVector;
VCtors[Type.Dictionary]      = DictionaryVector;

const partial0 = <T extends DataType, V extends VTypes[T['TType']]>(visit: (node: V) => any) => function(this: V) { return visit(this); };
const partial1 = <T extends DataType, V extends VTypes[T['TType']]>(visit: (node: V, a: any) => any) => function(this: V, a: any) { return visit(this, a); };
const partial2 = <T extends DataType, V extends VTypes[T['TType']]>(visit: (node: V, a: any, b: any) => any) => function(this: V, a: any, b: any) { return visit(this, a, b); };
// const partial3 = <T extends DataType, V extends VTypes[T['TType']]>(visit: (node: V, a: any, b: any, c: any) => any) => function(this: V, a: any, b: any, c: any) { return visit(this, a, b, c); };

// Bind and assign the Operator Visitor methods to each of the Vector subclasses for each Type
(Object.keys(Type) as Type[])
    .filter((typeId) => typeId > 0 || typeId === Type.Dictionary)
    .forEach((TType) => {
        type T = typeof TType;
        const VCtor = VCtors[TType];
        VCtor.prototype['get'] = partial1(Visitor.bindVisitor<T>(getVisitor, TType));
        VCtor.prototype['indexOf'] = partial2(Visitor.bindVisitor<T>(indexOfVisitor, TType));
        VCtor.prototype[Symbol.iterator] = partial0(Visitor.bindVisitor<T>(iteratorVisitor, TType));
    });
    
type VCtors = { [T in Type]: { new (...args: any[]): VTypes[T]; } };
interface VTypes {
           [Type.NONE]: NullVector;
           [Type.Null]: NullVector;
            [Type.Int]: IntVector;
          [Type.Float]: FloatVector;
         [Type.Binary]: BinaryVector;
           [Type.Utf8]: Utf8Vector;
           [Type.Bool]: BoolVector;
        [Type.Decimal]: DecimalVector;
           [Type.Date]: DateVector;
           [Type.Time]: TimeVector;
      [Type.Timestamp]: TimestampVector;
       [Type.Interval]: IntervalVector;
           [Type.List]: ListVector;
         [Type.Struct]: StructVector;
          [Type.Union]: UnionVector;
[Type.FixedSizeBinary]: FixedSizeBinaryVector;
  [Type.FixedSizeList]: FixedSizeListVector;
            [Type.Map]: MapVector;
     [Type.Dictionary]: DictionaryVector;
}
