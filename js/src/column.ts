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
import { Vector } from './interfaces';
import { clampRange } from './util/vector';
import { DataType, IterableArrayLike } from './type';

export interface Column<V extends Vector> {

    readonly name: string;
    readonly length: number;
    readonly type: V['type'];

    readonly nullCount: number;
    readonly numChildren: number;

    readonly chunks: V[];
    readonly field: Field<V['type']>;

    [Symbol.iterator](): IterableIterator<V['TValue'] | null>;

    search(index: number): [number, number] | null;
    search<N extends SearchContinuation<this>>(index: number, then: N): ReturnType<N> | null;

    isValid(index: number): boolean;
    get(index: number): V['TValue'] | null;
    slice(begin?: number, end?: number): this;
    toArray(): IterableArrayLike<V['TValue'] | null>;
    indexOf(element: V['TValue'], offset?: number): number;
    getChildAt<R extends DataType = any>(index: number): Column<Vector<R>> | null;
}

export class Column<V extends Vector> {
    static concat<T extends DataType>(...vectors: (Vector<T> | Column<Vector<T>>)[]) {
        const chunks = vectors.map((v) => v instanceof ChunkedVector ? v.chunks : v) as Vector<T>[];
        return new Column<Vector<T>>(new Field('', chunks[0].type, chunks.some((v) => v.nullCount > 0)), chunks);

    }
    constructor(field: Field<V['type']>, vectors: V[] = [], offsets?: Uint32Array) {
        return new ChunkedVector(field, vectors as any[], offsets) as any as Column<V>;
    }
}

type SearchContinuation<T extends Column<any>> = (column: T, chunkIndex: number, valueIndex: number) => any;

class ChunkedVector<T extends DataType = any> implements Column<Vector<T>> {

    public readonly field: Field<T>;
    public readonly chunks: Vector<T>[];

    public readonly length: number;
    public readonly numChildren: number;

    protected chunkOffsets: Uint32Array;
    protected _nullCount: number = -1;
    protected _children?: Column<any>[];

    constructor(field: Field<T>, vectors: Vector<T>[] = [], offsets = calculateOffsets(vectors)) {
        this.field = field;
        this.chunks = vectors;
        this.chunkOffsets = offsets;
        this.length = offsets[offsets.length - 1];
        this.numChildren = (this.type.children || []).length;
    }
    public get name() { return this.field.name; }
    public get type() { return this.field.type; }
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

    public getChildAt<R extends DataType = any>(index: number): Column<Vector<R>> | null {

        if (index < 0 || index >= this.numChildren) { return null; }

        let column, field: Field<R>, chunks: Vector<R>[];
        let columns = this._children || (this._children = []);

        if (column = columns[index]) { return column as Column<Vector<R>>; }
        if (field = ((this.type.children || [])[index] as Field<R>)) {
            chunks = this.chunks
                .map((vector) => vector.getChildAt<R>(index))
                .filter((vec): vec is Vector<R> => vec != null);
            if (chunks.length > 0) {
                return (columns[index] = new Column<Vector<R>>(field, chunks));
            }
        }

        return null;
    }

    public search(index: number): [number, number] | null;
    public search<N extends SearchContinuation<this>>(index: number, then: N): ReturnType<N> | null;
    public search<N extends SearchContinuation<this>>(idx: number, then?: N) {
        // binary search to find the child vector and value indices
        let offsets = this.chunkOffsets, rhs = offsets.length - 1;
        // return early if out of bounds, or if there's just one child
        if (idx < 0                 ) { return null; }
        if (idx >= offsets[rhs]) { return null; }
        if (rhs <= 1                ) { return then ? then(this, 0, idx) : [0, idx]; }
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
        return !!this.search(index, ({ chunks }, i, j) => chunks[i].isValid(j));
    }
    public get(index: number): T['TValue'] | null {
        return this.search(index, ({ chunks }, i, j) => chunks[i].get(j));
    }
    public indexOf(element: T['TValue'], offset?: number): number {
        let i = 0, start = 0, found = -1;
        let { chunks } = this, n = chunks.length;
        (offset && typeof offset === 'number') &&
            ([i, start] = (this.search(offset) || [0, 0]));
        do {
            if (~(found = chunks[i].indexOf(element, start))) {
                return found;
            }
            start = 0;
        } while (++i < n);
        return -1;
    }

    public concat(...others: (Vector<T> | Column<Vector<T>>)[]): Column<Vector<T>> {
        return Column.concat<T>(this as any, ...others);
    }

    public toArray(): IterableArrayLike<T['TValue'] | null> {
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
    public slice(begin?: number, end?: number): this {
        return clampRange(this, begin, end, this.sliceInternal) as this;
    }
    protected sliceInternal(column: this, offset: number, length: number) {
        const slices: Vector<T>[] = [];
        const { chunks, chunkOffsets } = column;
        for (let i = -1, n = chunks.length; ++i < n;) {
            const vector = chunks[i];
            const vectorOffset = chunkOffsets[i];
            const vectorLength = vector.length;
            // If the child is to the right of the slice boundary, exclude
            if (vectorOffset >= offset + length) { continue; }
            // If the child is to the left of of the slice boundary, exclude
            if (offset >= vectorOffset + vectorLength) { continue; }
            // If the child is between both left and right boundaries, include w/o slicing
            if (vectorOffset >= offset && (vectorOffset + vectorLength) <= offset + length) {
                slices.push(vector);
                continue;
            }
            // If the child overlaps one of the slice boundaries, include that slice
            const begin = Math.max(0, offset - vectorOffset);
            const end = begin + Math.min(vectorLength - begin, (offset + length) - vectorOffset);
            slices.push(vector.slice(begin, end));
        }
        return new Column(column.field, slices);
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
