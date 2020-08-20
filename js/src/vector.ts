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
import { DataType, strideForType } from './type';
import { instance as getVisitor } from './visitor/get';
import { instance as setVisitor } from './visitor/set';
import { instance as indexOfVisitor } from './visitor/indexof';
import { instance as toArrayVisitor } from './visitor/toarray';
import { instance as iteratorVisitor } from './visitor/iterator';
import { instance as byteLengthVisitor } from './visitor/bytelength';

export interface Vector<T extends DataType = any> {
    ///
    // Virtual properties for the TypeScript compiler.
    // These do not exist at runtime.
    ///
    readonly TType: T['TType'];
    readonly TArray: T['TArray'];
    readonly TValue: T['TValue'];
    /**
     * @summary Get and set elements by index.
     */
    [index: number]: T['TValue'] | null;
}

export class Vector<T extends DataType = any> {

    constructor(type: T, chunks: Data<T>[] = [new Data(type, 0, 0)], offsets?: Uint32Array) {
        this.type = type;
        this._chunks = chunks;
        switch (chunks.length) {
            case 0: this._offsets = new Uint32Array([0, 0]); break;
            case 1: this._offsets = new Uint32Array([0, chunks[0].length]); break;
            default: this._offsets = offsets ?? computeChunkOffsets(chunks); break;
        }
        this.stride = strideForType(type);
        this.numChildren = type.children?.length ?? 0;
        this.length = this._offsets[this._offsets.length - 1];
        Object.setPrototypeOf(this, vectorPrototypesByType[type.typeId]);
    }
    /**
     * @summary The primitive {@link Data `Data`} instances for this Vector's elements.
     */
    protected _chunks: Data<T>[];
    protected _offsets: Uint32Array;
    protected _nullCount!: number;
    protected _children?: Vector[];

    /**
     * @summary The {@link DataType `DataType`} of this Vector.
     */
    public readonly type: T;
    /**
     * @summary The number of elements in this Vector.
     */
    public readonly length: number;

    /**
     * @summary The number of primitive values per Vector element.
     */
    public readonly stride: number;

    /**
     * @summary The number of child Vectors this Vector has.
     */
    public readonly numChildren: number;

    /**
     * @summary The number of null elements in this Vector.
     */
    public get nullCount() {
        if (this._nullCount === -1) {
            this._nullCount = computeChunkNullCounts(this._chunks);
        }
        return this._nullCount;
    }
    /**
     * @summary The Array or TypedAray constructor used for the JS representation
     *  of the element's values in {@link Vector.prototype.toArray `toArray()`}.
     */
    public get ArrayType(): T['ArrayType'] { return this.type.ArrayType; }
    /**
     * @summary The name that should be printed when the Vector is logged in a message.
     */
    public get [Symbol.toStringTag]() {
        return `${this.VectorName}<${this.type[Symbol.toStringTag]}>`;
    }
    /**
     * @summary The name of this Vector.
     */
    public get VectorName() { return `${Type[this.type.typeId]}Vector`; }

    /**
     * @summary Check whether an element is null.
     * @param index The index at which to read the validity bitmap.
     */
    // @ts-ignore
    public isValid(index: number): boolean { return false; }
    /**
     * @summary Get an element value by position.
     * @param index The index of the element to read.
     */
    // @ts-ignore
    public get(index: number): T['TValue'] | null { return null; }
    /**
     * @summary Set an element value by position.
     * @param index The index of the element to write.
     * @param value The value to set.
     */
    // @ts-ignore
    public set(index: number, value: T['TValue'] | null): void { return; }
    /**
     * @summary Retrieve the index of the first occurrence of a value in an Vector.
     * @param element The value to locate in the Vector.
     * @param offset The index at which to begin the search. If offset is omitted, the search starts at index 0.
     */
    // @ts-ignore
    public indexOf(element: T['TValue'], offset?: number): number { return -1; }
    /**
     * @summary Get the size in bytes of an element by index.
     * @param index The index at which to get the byteLength.
     */
    // @ts-ignore
    public getByteLength(index: number): number { return 0; }
    /**
     * @summary Iterator for the Vector's elements.
     */
    public *[Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        const iterate = iteratorVisitor.getVisitFn(this.type.typeId);
        for (const chunk of this._chunks) {
            yield* iterate(chunk);
        }
    }
    /**
     * @summary Return a JavaScript Array or TypedArray of the Vector's elements.
     * 
     * @note If this Vector contains a single Data chunk and the Vector's type is a
     *  primitive numeric type corresponding to one of the JavaScript TypedArrays, this
     *  method returns a zero-copy slice of the underlying TypedArray values. If there's
     *  more than one chunk, the resulting TypedArray will be a copy of the data from each
     *  chunk's underlying TypedArray values.
     * 
     * @returns An Array or TypedArray of the Vector's elements, based on the Vector's DataType.
     */
    public toArray() {
        const chunks = this._chunks;
        const toArray = toArrayVisitor.getVisitFn(this.type.typeId);
        switch (chunks.length) {
            case 1: return toArray(chunks[0]);
            case 0: return new this.ArrayType();
        }
        let { ArrayType } = this;
        const arys = chunks.map(toArray);
        if (ArrayType !== arys[0].constructor) {
            ArrayType = arys[0].constructor;
        }
        return ArrayType === Array ? arys.flat(1) : arys.reduce((memo, ary) => {
            memo.ary.set(ary, memo.offset);
            memo.offset += ary.length;
            return memo;
        }, { ary: new ArrayType(this.length * this.stride), offset: 0 });
    }
    /**
     * @summary Returns a child Vector by name, or null if this Vector has no child with the given name.
     * @param name The name of the child to retrieve.
     */
    public getChild<R extends keyof T['TChildren']>(name: R) {
        return this.getChildAt(this.type.children?.findIndex((f) => f.name === name));
    }
    /**
     * @summary Returns a child Vector by index, or null if this Vector has no child at the supplied index.
     * @param index The index of the child to retrieve.
     */
    public getChildAt<R extends DataType = any>(index: number): Vector<R> | null {
        if (index > -1 && index < this.numChildren) {
            return (this._children ??= [])[index] ??= new Vector(
                (this.type.children[index] as Field<R>).type,
                this._chunks.map((data) => data.childData[index] as Data<R>),
                this._offsets.slice()
            );
        }
        return null;
    }
}

(Vector.prototype as any)._nullCount = -1;
(Vector.prototype as any)[Symbol.isConcatSpreadable] = true;

if (typeof Proxy !== 'undefined') {
    Object.setPrototypeOf(Vector.prototype, new Proxy({}, {
        get(target: {}, key: any, instance: any) {
            let p = key;
            switch (typeof key) {
                // fall through if key can be cast to a number
                // @ts-ignore
                case 'string': if ((p = +key) !== p) { break; }
                case 'number': return instance.get(p);
            }
            return Reflect.get(target, key, instance);
        },
        set(target: {}, key: any, value: any, instance: any) {
            let p = key;
            switch (typeof key) {
                // fall through if key can be cast to a number
                // @ts-ignore
                case 'string': if ((p = +key) !== p) { break; }
                case 'number': return (instance.set(p, value), true);
            }
            return Reflect.set(target, key, value, instance);
        }
    }));
}

const vectorPrototypesByType = Object
    .keys(Type).map((T: any) => Type[T] as any)
    .filter((T: any) => typeof T === 'number' && T !== Type.NONE)
    .reduce((prototypes, typeId) => ({
        ...prototypes,
        [typeId]: Object.create(Vector.prototype, {
            ['isValid']: { value: wrapCall1(isValid) },
            ['get']: { value: wrapCall1(wrapGet(getVisitor.getVisitFn(typeId))) },
            ['set']: { value: wrapCall2(wrapSet(setVisitor.getVisitFn(typeId))) },
            ['indexOf']: { value: wrapIndexOf(indexOfVisitor.getVisitFn(typeId)) },
            ['getByteLength']: { value: wrapCall1(byteLengthVisitor.getVisitFn(typeId)) },
        })
    }), {} as { [typeId: number]: any });

/** @ignore */
function computeChunkNullCounts<T extends DataType>(chunks: Data<T>[]) {
    return chunks.reduce((nullCount, chunk) => {
        return nullCount + chunk.nullCount;
    }, 0);
}

/** @ignore */
function computeChunkOffsets<T extends DataType>(chunks: Data<T>[]) {
    return chunks.reduce((offsets, chunk, index) => {
        offsets[index + 1] = offsets[index] + chunk.length;
        return offsets;
    }, new Uint32Array(chunks.length + 1));
}

/** @ignore */
function binarySearch<
    T extends DataType,
    F extends (chunks: Data<T>[], _1: number, _2: number) => any
>(chunks: Data<T>[], offsets: Uint32Array, idx: number, fn: F) {
    let lhs = 0, mid = 0, rhs = offsets.length - 1;
    do {
        if (lhs >= rhs - 1) {
            // return fn(this, lhs, idx - offsets[lhs]);
            return (idx < offsets[rhs]) ? fn(chunks, lhs, idx - offsets[lhs]) : null;
        }
        mid = lhs + (((rhs - lhs) * .5) | 0);
        idx < offsets[mid] ? (rhs = mid) : (lhs = mid);
    } while (lhs < rhs);
}

/** @ignore */
function isValid<T extends DataType>(data: Data<T>, index: number): boolean {
    return data.getValid(index);
}

/** @ignore */
function wrapGet<T extends DataType, F extends (data: Data<T>, _1: any) => any>(fn: F) {
    return (data: Data<T>, _1: any) => data.getValid(_1) ? fn(data, _1) : null;
}

/** @ignore */
function wrapSet<T extends DataType, F extends (data: Data<T>, _1: any, _2: any) => void>(fn: F) {
    return (data: Data<T>, _1: any, _2: any) => {
        if (data.setValid(_1, !(_2 === null || _2 === undefined))) {
            return fn(data, _1, _2);
        }
    };
}

/** @ignore */
function wrapCall1<T extends DataType, F extends (c: Data<T>, _1: number) => any>(fn: F) {
    function chunkedFn(chunks: Data<T>[], i: number, j: number) { return fn(chunks[i], j); }
    return function(this: Vector<T>, index: number) {
        return binarySearch(this._chunks, this._offsets, index, chunkedFn);
    };
}

/** @ignore */
function wrapCall2<T extends DataType, F extends (c: Data<T>, _1: number, _2: any) => any>(fn: F) {
    let _1: any;
    function chunkedFn(chunks: Data<T>[], i: number, j: number) { return fn(chunks[i], j, _1); }
    return function(this: Vector<T>, index: number, value: any) {
        _1 = value;
        const result = binarySearch(this._chunks, this._offsets, index, chunkedFn);
        _1 = undefined;
        return result;
    };
}

/** @ignore */
function wrapIndexOf<T extends DataType, F extends (c: Data<T>, e: T['TValue'], o?: number) => any>(indexOf: F) {
    let _1: any;
    function chunkedIndexOf(chunks: Data<T>[], chunkIndex: number, fromIndex: number) {
        let begin = fromIndex, index = 0, total = 0;
        for (let i = chunkIndex - 1, n = chunks.length; ++i < n;) {
            const chunk = chunks[i];
            if (~(index = indexOf(chunk, _1, begin))) {
                return total + index;
            }
            begin = 0;
            total += chunk.length;
        }
        return -1;
    }
    return function(this: Vector<T>, element: T['TValue'], offset?: number) {
        _1 = element;
        const result = typeof offset !== 'number'
            ? chunkedIndexOf(this._chunks, 0, 0)
            : binarySearch(this._chunks, this._offsets, offset, chunkedIndexOf);
        _1 = undefined;
        return result;
    }
}


// import { Chunked } from './vector/chunked';

// /** @ignore */
// export interface Clonable<R extends AbstractVector> {
//     clone(...args: any[]): R;
// }

// /** @ignore */
// export interface Sliceable<R extends AbstractVector> {
//     slice(begin?: number, end?: number): R;
// }

// /** @ignore */
// export interface Applicative<T extends DataType, R extends Chunked> {
//     concat(...others: Vector<T>[]): R;
//     readonly [Symbol.isConcatSpreadable]: boolean;
// }

// export interface AbstractVector<T extends DataType = any>
//     extends Clonable<Vector<T>>,
//             Sliceable<Vector<T>>,
//             Applicative<T, Chunked<T>> {

//     readonly TType: T['TType'];
//     readonly TArray: T['TArray'];
//     readonly TValue: T['TValue'];
// }

// export abstract class AbstractVector<T extends DataType = any> implements Iterable<T['TValue'] | null> {

//     public abstract readonly data: Data<T>;
//     public abstract readonly type: T;
//     public abstract readonly typeId: T['TType'];
//     public abstract readonly length: number;
//     public abstract readonly stride: number;
//     public abstract readonly nullCount: number;
//     public abstract readonly byteLength: number;
//     public abstract readonly numChildren: number;

//     public abstract readonly ArrayType: T['ArrayType'];

//     public abstract isValid(index: number): boolean;
//     public abstract get(index: number): T['TValue'] | null;
//     public abstract set(index: number, value: T['TValue'] | null): void;
//     public abstract indexOf(value: T['TValue'] | null, fromIndex?: number): number;
//     public abstract [Symbol.iterator](): IterableIterator<T['TValue'] | null>;

//     public abstract toArray(): T['TArray'];
//     public abstract getChildAt<R extends DataType = any>(index: number): Vector<R> | null;
// }

// export { AbstractVector as Vector };
