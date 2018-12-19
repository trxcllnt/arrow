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
import { Vector } from '../vector';
import { DataType } from '../type';
import { ChunkedVector } from './chunked';
import { clampRange } from '../util/vector';

export abstract class BaseVector<T extends DataType = any> extends Vector<T> {

    // @ts-ignore
    protected _data: Data<T>;
    protected _stride: number = 1;
    protected _numChildren: number = 0;
    protected _children?: Vector[];

    constructor(data: Data<T>, children?: Vector[], stride?: number) {
        super();
        this._children = children;
        this._numChildren = data.childData.length;
        this._bindDataAccessors(this._data = data);
        this._stride = Math.floor(Math.max(stride || 1, 1));
    }

    public get data() { return this._data; }
    public get stride() { return this._stride; }
    public get numChildren() { return this._numChildren; }

    public get type() { return this.data.type; }
    public get typeId() { return this.data.typeId as T['TType']; }
    public get length() { return this.data.length; }
    public get offset() { return this.data.offset; }
    public get nullCount() { return this.data.nullCount; }
    public get VectorName() { return this.constructor.name; }

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
        return clampRange(this, begin, end, this.sliceInternal) as any;
    }

    protected sliceInternal(vector: BaseVector<T>, offset: number, length: number) {
        const stride = vector.stride;
        return vector.clone(vector.data.slice(offset * stride, (length - offset) * stride));
    }

    // @ts-ignore
    protected _bindDataAccessors(data: Data<T>) {
        if (this.nullCount > 0) {
            this['get'] && (this['get'] = wrapNullable1(this['get']));
        }
    }
}

function wrapNullable1<T extends DataType, V extends Vector<T>, F extends (i: number) => any>(fn: F): (...args: Parameters<F>) => ReturnType<F> {
    return function(this: V, i: number) { return this.isValid(i) ? fn.call(this, i) : null; };
}
