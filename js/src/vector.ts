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

import { Type } from './enum';
import { clampRange } from './util/vector';
import { DataType, strideForType } from './type';
import { Data, makeData, DataProps } from './data';

import {
    ChunkedIterator,
    isChunkedValid,
    computeChunkOffsets,
    computeChunkNullCounts,
    sliceChunks,
    wrapChunkedCall1,
    wrapChunkedCall2,
    wrapChunkedIndexOf,
} from './util/chunk';

import { NumericIndexingProxyHandlerMixin } from './util/proxy';

import { instance as getVisitor } from './visitor/get';
import { instance as setVisitor } from './visitor/set';
import { instance as indexOfVisitor } from './visitor/indexof';
import { instance as toArrayVisitor } from './visitor/toarray';
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
     * @see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Symbol/isConcatSpreadable
     */
    [Symbol.isConcatSpreadable]: true;
}

const vectorPrototypesByTypeId = {} as { [typeId: number]: any };

export class Vector<T extends DataType = any> {

    constructor(...args: Data<T>[]);
    constructor(...args: Vector<T>[]);
    constructor(...args: (readonly (Data<T> | Vector<T>)[])[]);
    constructor(...args: any[]) {
        const data = args.flat().flatMap((x) => {
            return x instanceof Data ? [x]
                : x instanceof Vector ? x.data
                    : makeVector(x as Vector<T>).data;
        });
        if (data.some((x) => !(x instanceof Data))) {
            throw new TypeError('Vector constructor expects an Array of Data instances.');
        }
        this.data = data;
        this.type = data[0].type;
        switch (data.length) {
            case 0: this._offsets = new Uint32Array([0]); break;
            case 1: this._offsets = new Uint32Array([0, data[0].length]); break;
            default: this._offsets = computeChunkOffsets(data); break;
        }
        this.stride = strideForType(this.type);
        this.numChildren = this.type.children?.length ?? 0;
        this.length = this._offsets[this._offsets.length - 1];
        Object.setPrototypeOf(this, vectorPrototypesByTypeId[this.type.typeId]);
    }

    declare protected _offsets: Uint32Array;
    declare protected _nullCount: number;
    declare protected _byteLength: number;

    /**
     * @summary Get and set elements by index.
     */
    [index: number]: T['TValue'] | null;

    /**
     * @summary The {@link DataType `DataType`} of this Vector.
     */
    public readonly type: T;

    /**
     * @summary The primitive {@link Data `Data`} instances for this Vector's elements.
     */
    public readonly data: ReadonlyArray<Data<T>>;

    /**
     * @summary The number of elements in this Vector.
     */
    public readonly length: number;

    /**
     * @summary The number of primitive values per Vector element.
     */
    public readonly stride: number;

    /**
     * @summary The number of child Vectors if this Vector is a nested dtype.
     */
    public readonly numChildren: number;

    /**
     * @summary The aggregate size (in bytes) of this Vector's buffers and/or child Vectors.
     */
    public get byteLength() {
        if (this._byteLength === -1) {
            this._byteLength = this.data.reduce((byteLength, data) => byteLength + data.byteLength, 0);
        }
        return this._byteLength;
    }

    /**
     * @summary The number of null elements in this Vector.
     */
    public get nullCount() {
        if (this._nullCount === -1) {
            this._nullCount = computeChunkNullCounts(this.data);
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
    public [Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        return new ChunkedIterator(this.data);
    }

    /**
     * @summary Combines two or more Vectors of the same type.
     * @param others Additional Vectors to add to the end of this Vector.
     */
    public concat(...others: Vector<T>[]): Vector<T> {
        return new Vector(this.data.concat(others.map((x) => x.data).flat()));
    }

    /**
     * Return a zero-copy sub-section of this Vector.
     * @param start The beginning of the specified portion of the Vector.
     * @param end The end of the specified portion of the Vector. This is exclusive of the element at the index 'end'.
     */
    public slice(begin?: number, end?: number): Vector<T> {
        return new Vector(clampRange(this, begin, end, ({ data, _offsets }, begin, end) => {
            return sliceChunks(data, _offsets, begin, end);
        }));
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
        const data = this.data;
        const toArray = toArrayVisitor.getVisitFn(this.type.typeId);
        switch (data.length) {
            case 1: return toArray(data[0]);
            case 0: return new this.ArrayType();
        }
        let { ArrayType } = this;
        const arrays = data.map(toArray);
        if (ArrayType !== arrays[0].constructor) {
            ArrayType = arrays[0].constructor;
        }
        return ArrayType === Array ? arrays.flat(1) : arrays.reduce((memo, array) => {
            memo.array.set(array, memo.offset);
            memo.offset += array.length;
            return memo;
        }, { array: new ArrayType(this.length * this.stride), offset: 0 });
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
            return new Vector(this.data.map(({ children }) => children[index] as Data<R>));
        }
        return null;
    }

    // Initialize this static property via an IIFE so bundlers don't tree-shake
    // out this logic, but also so we're still compliant with `"sideEffects": false`
    protected static [Symbol.toStringTag] = ((proto: Vector) => {
        (proto as any)._nullCount = -1;
        (proto as any)._byteLength = -1;
        (proto as any)[Symbol.isConcatSpreadable] = true;
        Object.setPrototypeOf(proto, new Proxy({}, new NumericIndexingProxyHandlerMixin(
            (inst, key) => inst.get(key),
            (inst, key, val) => inst.set(key, val)
        )));

        Object.assign(vectorPrototypesByTypeId, Object
            .keys(Type).map((T: any) => Type[T] as any)
            .filter((T: any) => typeof T === 'number' && T !== Type.NONE)
            .reduce((prototypes, typeId) => ({
                ...prototypes,
                [typeId]: Object.create(proto, {
                    ['isValid']: { value: wrapChunkedCall1(isChunkedValid) },
                    ['get']: { value: wrapChunkedCall1(getVisitor.getVisitFnByTypeId(typeId)) },
                    ['set']: { value: wrapChunkedCall2(setVisitor.getVisitFnByTypeId(typeId)) },
                    ['indexOf']: { value: wrapChunkedIndexOf(indexOfVisitor.getVisitFnByTypeId(typeId)) },
                    ['getByteLength']: { value: wrapChunkedCall1(byteLengthVisitor.getVisitFnByTypeId(typeId)) },
                })
            }), {}));

        return 'Vector';
    })(Vector.prototype);
}

import * as dtypes from './type';

export function makeVector(data: Int8Array | readonly Int8Array[]): Vector<dtypes.Int8>;
export function makeVector(data: Int16Array | readonly Int16Array[]): Vector<dtypes.Int16>;
export function makeVector(data: Int32Array | readonly Int32Array[]): Vector<dtypes.Int32>;
export function makeVector(data: BigInt64Array | readonly BigInt64Array[]): Vector<dtypes.Int64>;
export function makeVector(data: Uint8Array | readonly Uint8Array[]): Vector<dtypes.Uint8>;
export function makeVector(data: Uint16Array | readonly Uint16Array[]): Vector<dtypes.Uint16>;
export function makeVector(data: Uint32Array | readonly Uint32Array[]): Vector<dtypes.Uint32>;
export function makeVector(data: BigUint64Array | readonly BigUint64Array[]): Vector<dtypes.Uint64>;
export function makeVector(data: Float32Array | readonly Float32Array[]): Vector<dtypes.Float32>;
export function makeVector(data: Float64Array | readonly Float64Array[]): Vector<dtypes.Float64>;
export function makeVector<T extends DataType>(data: Data<T> | readonly Data<T>[]): Vector<T>;
export function makeVector<T extends DataType>(data: Vector<T> | readonly Vector<T>[]): Vector<T>;
export function makeVector<T extends DataType>(data: DataProps<T> | readonly DataProps<T>[]): Vector<T>;

export function makeVector(init: any) {
    if (init) {
        if (init instanceof Data) { return new Vector(init); }
        if (init instanceof Vector) { return new Vector(init.data); }
        if (init.type instanceof DataType) { return new Vector([makeData(init)]); }
        if (Array.isArray(init)) {
            return new Vector(init.flat().flatMap((x) => x instanceof Vector ? x.data : makeVector(x).data));
        }
        if (ArrayBuffer.isView(init)) {
            if (init instanceof DataView) {
                init = new Uint8Array(init.buffer);
            }
            const props = { offset: 0, length: init.length, nullCount: 0, data: init };
            if (init instanceof Int8Array) { return new Vector([makeData({ ...props, type: new dtypes.Int8 })]); }
            if (init instanceof Int16Array) { return new Vector([makeData({ ...props, type: new dtypes.Int16 })]); }
            if (init instanceof Int32Array) { return new Vector([makeData({ ...props, type: new dtypes.Int32 })]); }
            if (init instanceof BigInt64Array) { return new Vector([makeData({ ...props, type: new dtypes.Int64 })]); }
            if (init instanceof Uint8Array) { return new Vector([makeData({ ...props, type: new dtypes.Uint8 })]); }
            if (init instanceof Uint16Array) { return new Vector([makeData({ ...props, type: new dtypes.Uint16 })]); }
            if (init instanceof Uint32Array) { return new Vector([makeData({ ...props, type: new dtypes.Uint32 })]); }
            if (init instanceof BigUint64Array) { return new Vector([makeData({ ...props, type: new dtypes.Uint64 })]); }
            if (init instanceof Float32Array) { return new Vector([makeData({ ...props, type: new dtypes.Float32 })]); }
            if (init instanceof Float64Array) { return new Vector([makeData({ ...props, type: new dtypes.Float64 })]); }
            throw new Error('Unrecognized input');
        }
    }
    throw new Error('Unrecognized input');
}
