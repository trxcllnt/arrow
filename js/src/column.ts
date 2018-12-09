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
import { Vector } from './vector';
import { clampRange } from './util/vector';
import { DataType, RowLike } from './type';
import { MapVector, StructVector } from './vector';

type SearchContinuation<T extends ChunkedVector> = (column: T, chunkIndex: number, valueIndex: number) => any;

export class ChunkedVector<T extends DataType = any> extends Vector<T> {

    static flatten<T extends DataType>(...vectors: Vector<T>[]) {
        return vectors.reduce(function flatten(xs: any[], x: any): any[] {
            return x instanceof ChunkedVector ? x.chunks.reduce(flatten, xs) : [...xs, x];
        }, []).filter((x: any): x is Vector<T> => x instanceof Vector);
    }

    static concat<T extends DataType>(...vectors: Vector<T>[]): Vector<T> {
        return new ChunkedVector(ChunkedVector.flatten(...vectors));
    }

    public readonly length: number;
    public readonly numChildren: number;
    public readonly chunks: Vector<T>[];

    protected _children?: Column[];
    protected _nullCount: number = -1;
    protected chunkOffsets: Uint32Array;

    constructor(chunks: Vector<T>[] = [], offsets = calculateOffsets(chunks)) {
        super();
        this.chunks = chunks;
        this.chunkOffsets = offsets;
        this.length = offsets[offsets.length - 1];
        this.numChildren = (this.type.children || []).length;
    }

    public get data() { return this.chunks[0].data; }
    public get type() { return this.chunks[0].type; }
    public get TType() { return this.chunks[0].TType; }
    public get TArray() { return this.chunks[0].TArray; }
    public get TValue() { return this.chunks[0].TValue; }
    public get ArrayType() { return this.chunks[0].ArrayType; }

    public get nullCount() {
        let nullCount = this._nullCount;
        if (nullCount < 0) {
            this._nullCount = nullCount = this.chunks.reduce((x, { nullCount }) => x + nullCount, 0);
        }
        return nullCount;
    }

    public *[Symbol.iterator](): IterableIterator<T['TValue'] | null> {
        for (const chunk of this.chunks) {
            yield* chunk;
        }
    }

    public concat(...others: Vector<T>[]): Vector<T> {
        return ChunkedVector.concat<T>(this, ...others);
    }

    public getChildAt<R extends DataType = any>(index: number): Column<R> | null {

        if (index < 0 || index >= this.numChildren) { return null; }

        let columns = this._children || (this._children = []);
        let column: Column<R>, field: Field<R>, chunks: Vector<R>[];

        if (column = columns[index]) { return column; }
        if (field = ((this.type.children || [])[index] as Field<R>)) {
            chunks = this.chunks
                .map((vector) => vector.getChildAt<R>(index))
                .filter((vec): vec is Vector<R> => vec != null);
            if (chunks.length > 0) {
                return (columns[index] = new Column<R>(field, chunks));
            }
        }

        return null;
    }

    public search(index: number): [number, number] | null;
    public search<N extends SearchContinuation<ChunkedVector<T>>>(index: number, then?: N): ReturnType<N> | null;
    public search<N extends SearchContinuation<ChunkedVector<T>>>(index: number, then?: N) {
        let idx = index;
        // binary search to find the child vector and value indices
        let offsets = this.chunkOffsets, rhs = offsets.length - 1;
        // return early if out of bounds, or if there's just one child
        if (idx < 0            ) { return null; }
        if (idx >= offsets[rhs]) { return null; }
        if (rhs <= 1           ) { return then ? then(this, 0, idx) : [0, idx]; }
        let lhs = 0, pos = 0, mid = 0;
        do {
            if (lhs + 1 === rhs) {
                return then ? then(this, lhs, idx - pos) : [lhs, idx - pos];
            }
            mid = lhs + ((rhs - lhs) / 2) | 0;
            idx >= offsets[mid] ? (lhs = mid) : (rhs = mid);
        } while (idx < offsets[rhs] && idx >= (pos = offsets[lhs]));
        return null;
    }

    public isValid(index: number): boolean {
        return !!this.search(index, this.isValidInternal);
    }

    public get(index: number): T['TValue'] | null {
        return this.search(index, this.getInternal);
    }

    public indexOf(element: T['TValue'], offset?: number): number {
        if (offset && typeof offset === 'number') {
            return this.search(offset, (self, i, j) => this.indexOfInternal(self, i, j, element))!;
        }
        return this.indexOfInternal(this, 0, Math.max(0, offset || 0), element);
    }

    public toArray(): T['TArray'] {
        const { chunks } = this;
        const n = chunks.length;
        const { ArrayType } = this.type;
        if (n <= 0) { return new ArrayType(0); }
        if (n <= 1) { return chunks[0].toArray(); }
        let len = 0, src = new Array(n);
        for (let i = -1; ++i < n;) {
            len += (src[i] = chunks[i].toArray()).length;
        }
        let dst = new (ArrayType as any)(len);
        let set: any = ArrayType === Array ? arraySet : typedSet;
        for (let i = -1, idx = 0; ++i < n;) {
            idx = set(src[i], dst, idx);
        }
        return dst;
    }

    public slice(begin?: number, end?: number): ChunkedVector<T> {
        return clampRange(this, begin, end, this.sliceInternal);
    }

    protected getInternal({ chunks }: ChunkedVector<T>, i: number, j: number) { return chunks[i].get(j); }
    protected isValidInternal({ chunks }: ChunkedVector<T>, i: number, j: number) { return chunks[i].isValid(j); }
    protected indexOfInternal({ chunks }: ChunkedVector<T>, chunkIndex: number, fromIndex: number, element: T['TValue']) {
        let start = fromIndex, found = -1;
        let i = chunkIndex - 1, n = chunks.length;
        while (++i < n) {
            if (~(found = chunks[i].indexOf(element, start))) {
                return found;
            }
            start = 0;
        }
        return -1;
    }

    protected sliceInternal(column: ChunkedVector<T>, offset: number, length: number) {
        const slices: Vector<T>[] = [];
        const { chunks, chunkOffsets } = column;
        for (let i = -1, n = chunks.length; ++i < n;) {
            const chunk = chunks[i];
            const chunkOffset = chunkOffsets[i];
            const chunkLength = chunk.length;
            // If the child is to the right of the slice boundary, exclude
            if (chunkOffset >= offset + length) { continue; }
            // If the child is to the left of of the slice boundary, exclude
            if (offset >= chunkOffset + chunkLength) { continue; }
            // If the child is between both left and right boundaries, include w/o slicing
            if (chunkOffset >= offset && (chunkOffset + chunkLength) <= offset + length) {
                slices.push(chunk);
                continue;
            }
            // If the child overlaps one of the slice boundaries, include that slice
            const begin = Math.max(0, offset - chunkOffset);
            const end = begin + Math.min(chunkLength - begin, (offset + length) - chunkOffset);
            slices.push(chunk.slice(begin, end) as Vector<T>);
        }
        return new ChunkedVector(slices);
    }
}

export class Column<T extends DataType = any> extends ChunkedVector<T> {

    constructor(field: Field<T>, vectors: Vector<T>[] = [], offsets?: Uint32Array) {
        super(ChunkedVector.flatten(...vectors), offsets);
        this.field = field;
    }

    public readonly field: Field<T>;
    public get name() { return this.field.name; }

    public slice(begin?: number, end?: number): Column<T> {
        return new Column(this.field, super.slice(begin, end).chunks);
    }
}

const columnDescriptor = { enumerable: true, configurable: false, get: () => {} };
const lengthDescriptor = { writable: false, enumerable: false, configurable: false, value: -1 };
const rowIndexDescriptor = { writable: false, enumerable: false, configurable: true, value: null as any };
const rowParentDescriptor = { writable: false, enumerable: false, configurable: false, value: null as any };
const row = { parent: rowParentDescriptor, rowIndex: rowIndexDescriptor };

export class Row<T extends { [key: string]: DataType }> implements Iterable<T[keyof T]['TValue']> {
    static new<T extends { [key: string]: DataType }>(schemaOrFields: T | Field[], fieldsAreEnumerable = false): RowLike<T> & Row<T> {
        let schema: T, fields: Field[];
        if (Array.isArray(schemaOrFields)) {
            fields = schemaOrFields;
        } else {
            schema = schemaOrFields;
            fieldsAreEnumerable = true;
            fields = Object.keys(schema).map((x) => new Field(x, schema[x]));
        }
        return new Row<T>(fields, fieldsAreEnumerable) as RowLike<T> & Row<T>;
    }
    // @ts-ignore
    private parent: TParent;
    // @ts-ignore
    private rowIndex: number;
    // @ts-ignore
    public readonly length: number;
    private constructor(fields: Field[], fieldsAreEnumerable: boolean) {
        lengthDescriptor.value = fields.length;
        Object.defineProperty(this, 'length', lengthDescriptor);
        fields.forEach((field, columnIndex) => {
            columnDescriptor.get = this._bindGetter(columnIndex);
            columnDescriptor.enumerable = fieldsAreEnumerable;
            Object.defineProperty(this, field.name, columnDescriptor);
            columnDescriptor.enumerable = !fieldsAreEnumerable;
            Object.defineProperty(this, columnIndex, columnDescriptor);
            columnDescriptor.get = false as any;
        });
    }
    *[Symbol.iterator](this: RowLike<T>) {
        for (let i = -1, n = this.length; ++i < n;) {
            yield this[i];
        }
    }
    private _bindGetter(colIndex: number) {
        return function (this: Row<T>) {
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
        return bound as RowLike<T>;
    }
}

function calculateOffsets<T extends DataType>(vectors: Vector<T>[]) {
    let offsets = new Uint32Array((vectors || []).length + 1);
    let offset = offsets[0] = 0, length = offsets.length;
    for (let index = 0; ++index < length;) {
        offsets[index] = (offset += vectors[index - 1].length);
    }
    return offsets;
}

const typedSet = (src: TypedArray, dst: TypedArray, offset: number) => {
    dst.set(src, offset);
    return (offset + src.length);
};

const arraySet = (src: any[], dst: any[], offset: number) => {
    let idx = offset - 1;
    for (let i = -1, n = src.length; ++i < n;) {
        dst[++idx] = src[i];
    }
    return idx;
};

interface TypedArray extends ArrayBufferView {
    readonly length: number;
    readonly [n: number]: number;
    set(array: ArrayLike<number>, offset?: number): void;
}
