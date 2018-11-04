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
import { Schema } from './schema';
import { Column } from './column';
import { DataType, Struct } from './type';
import { Vector, VectorLike } from './interfaces';
import { clampRange } from './util/vector';
import { Vector as ArrowVector } from './vector';
import { instance as getVisitor } from './visitor/get';
import { instance as indexOfVisitor } from './visitor/indexof';
import { instance as toArrayVisitor } from './visitor/toarray';
import { instance as iteratorVisitor } from './visitor/iterator';
// import { instance as byteWidthVisitor } from './visitor/bytewidth';

export class RecordBatch<T extends { [key: string]: DataType } = any> implements VectorLike<Struct<T>> {

//     public static from<T extends { [key: string]: DataType } = any>(vectors: Vector[]) {
//         const numRows = Math.max(...vectors.map((v) => v.length));
//         return new RecordBatch<T>(Schema.from(vectors), numRows, vectors);
//     }

    static new<T extends { [key: string]: DataType } = any>(schema: Schema, numRows: number, columns: (Data | Vector)[]) {
        const childData = columns.map((x) => x instanceof ArrowVector ? x.data : x);
        const data = Data.Struct(new Struct(schema.fields), 0, numRows, 0, null, childData);
        return new RecordBatch<T>(schema, data);
    }

    public readonly schema: Schema;
    public readonly numCols: number;
    public readonly data: Data<Struct<T>>;
    protected _children: Vector[] | void;
    
    constructor(schema: Schema, data: Data<Struct<T>>, children?: Vector[]) {
        this.data = data;
        this.schema = schema;
        this._children = children;
        this.numCols = schema.fields.length;
    }
    public get length() { return this.data.length; }
    public clone<R extends { [key: string]: DataType } = any>(data: Data<Struct<R>>, children = this._children) {
        return new RecordBatch(this.schema, data as any, children as any);
    }
    public concat(...others: VectorLike<Struct<T>>[]): VectorLike<Struct<T>> {
        return Column.concat(this as any, ...others as any[]);
    }
    public isValid(_index: number): boolean { return true; }
    public getChildAt<R extends DataType = any>(index: number): Vector<R> | null {
        return index < 0 || index >= this.numCols ? null : (
            (this._children || (this._children = []))[index] ||
            (this._children[index] = ArrowVector.new<R>(this.data.childData[index] as Data<R>))
        ) as Vector<R>;
    }
    public slice(begin?: number, end?: number): VectorLike<Struct<T>> {
        // Adjust args similar to Array.prototype.slice. Normalize begin/end to
        // clamp between 0 and length, and wrap around on negative indices, e.g.
        // slice(-1, 5) or slice(5, -1)
        return clampRange(this, begin, end, (x, y, z) => x.clone(x.data.slice(y, z))) as any;
    }
    public get(index: number): Struct<T>['TValue'] | null {
        return getVisitor.visit(this as any as Vector<Struct<T>>, index);
    }
    public indexOf(value: Struct<T>['TValue'] | null, fromIndex?: number): number {
        return indexOfVisitor.visit(this as any as Vector<Struct<T>>, value, fromIndex);
    }
    public toArray(): Struct<T>['TArray'] {
        return toArrayVisitor.visit(this as any as Vector<Struct<T>>);
    }
    public [Symbol.iterator](): IterableIterator<Struct<T>['TValue'] | null> {
        return iteratorVisitor.visit(this as any as Vector<Struct<T>>);
    }

    public select<K extends keyof T = any>(...columnNames: K[]) {
        const fields = this.schema.fields;
        const namesToKeep = columnNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
        return RecordBatch.new<{ [P in K]: T[P] }>(
            this.schema.select(...columnNames), this.length,
            this.data.childData.filter((_, i) => namesToKeep[fields[i].name])
        );
    }
//     public rowsToString(separator = ' | ', rowOffset = 0, maxColumnWidths: number[] = []) {
//         return new PipeIterator(recordBatchRowsToString(this, separator, rowOffset, maxColumnWidths), 'utf8');
//     }
// }

// function* recordBatchRowsToString(recordBatch: RecordBatch, separator = ' | ', rowOffset = 0, maxColumnWidths: number[] = []) {
//     const fields = recordBatch.schema.fields;
//     const header = ['row_id', ...fields.map((f) => `${f}`)].map(valueToString);
//     header.forEach((x, i) => {
//         maxColumnWidths[i] = Math.max(maxColumnWidths[i] || 0, x.length);
//     });
//     // Pass one to convert to strings and count max column widths
//     for (let i = -1, n = recordBatch.length - 1; ++i < n;) {
//         let val, row = [rowOffset + i, ...recordBatch.get(i) as Struct['TValue']];
//         for (let j = -1, k = row.length; ++j < k; ) {
//             val = valueToString(row[j]);
//             maxColumnWidths[j] = Math.max(maxColumnWidths[j] || 0, val.length);
//         }
//     }
//     for (let i = -1; ++i < recordBatch.length;) {
//         if ((rowOffset + i) % 1000 === 0) {
//             yield header.map((x, j) => leftPad(x, ' ', maxColumnWidths[j])).join(separator);
//         }
//         yield [rowOffset + i, ...recordBatch.get(i) as Struct['TValue']]
//             .map((x) => valueToString(x))
//             .map((x, j) => leftPad(x, ' ', maxColumnWidths[j]))
//             .join(separator);
//     }
}
