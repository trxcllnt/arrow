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
import { Vector } from './vector';
import { Schema, Field } from './schema';
import { StructVector } from './vector';
import { DataType, Struct } from './type';
import { Vector as VType } from './interfaces';

export class RecordBatch<T extends { [key: string]: DataType } = any> extends Vector<Struct<T>> {

    public static from<T extends { [key: string]: DataType } = any>(vectors: VType<T[keyof T]>[], names: string[] = []) {
        return new RecordBatch<T>(
            Schema.from<T>(vectors, names),
            vectors.reduce((len, vec) => Math.max(len, vec.length), 0),
            vectors
        );
    }
  
    private impl: StructVector<T>;
    public readonly schema: Schema;

    constructor(schema: Schema<T>, numRows: number, childData: (Data | Vector)[]);
    constructor(schema: Schema<T>, data: Data<Struct<T>>, children?: Vector[]);
    constructor(...args: any[]) {
        super();
        this.schema = args[0];
        let data: Data<Struct<T>>;
        let children: Vector[] | undefined;
        if (typeof args[1] === 'number') {
            const fields = this.schema.fields as Field<T[keyof T]>[];
            const [, numRows, childData] = args as [Schema<T>, number, Data[]];
            data = Data.Struct(new Struct<T>(fields), 0, numRows, 0, null, childData);
        } else {
            [, data, children] = (args as [Schema<T>, Data<Struct<T>>, Vector[]?]);
        }
        this.impl = new StructVector(data, children);
    }

    public clone<R extends { [key: string]: DataType } = any>(data: Data<Struct<R>>, children = (this.impl as any).children) {
        return new RecordBatch<R>(this.schema, data, children);
    }

    public get type() { return this.impl.type; }
    public get data() { return this.impl.data; }
    public get length() { return this.impl.length; }
    public get numCols() { return this.schema.fields.length; }
    public get rowProxy() { return this.impl.rowProxy; }
    public get nullCount() { return this.impl.nullCount; }
    public get numChildren() { return this.impl.numChildren; }

    public get TType() { return this.impl.TType; }
    public get TArray() { return this.impl.TArray; }
    public get TValue() { return this.impl.TValue; }
    public get ArrayType() { return this.impl.ArrayType; }

    public get(index: number) { return this.impl.get(index); }
    public isValid(index: number) { return this.impl.isValid(index); }
    public indexOf(value: Struct<T>['TValue'] | null, fromIndex?: number) { return this.impl.indexOf(value, fromIndex); }

    public toArray() { return this.impl.toArray(); }
    public [Symbol.iterator]() { return this.impl[Symbol.iterator](); }

    public slice(begin?: number, end?: number): RecordBatch<T> {
        return this.impl.slice.call(this, begin, end) as RecordBatch<T>;
    }

    public concat(...others: Vector<Struct<T>>[]): Vector<Struct<T>> {
        return this.impl.concat(...others.map((x) => x instanceof RecordBatch ? x.impl : x) as Vector<Struct<T>>[]);
    }

    public getChildAt<R extends DataType = any>(index: number) { return this.impl.getChildAt<R>(index); }

    public select<K extends keyof T = any>(...columnNames: K[]) {
        const fields = this.schema.fields;
        const schema = this.schema.select(...columnNames);
        const childNames = columnNames.reduce((xs, x) => (xs[x] = true) && xs, <any> {});
        const childData = this.data.childData.filter((_, i) => childNames[fields[i].name]);
        const structData = Data.Struct(new Struct(schema.fields), 0, this.length, 0, null, childData);
        return new RecordBatch<{ [P in K]: T[P] }>(schema, structData);
    }
//     public rowsToString(separator = ' | ', rowOffset = 0, maxColumnWidths: number[] = []) {
//         return new PipeIterator(recordBatchRowsToString(this, separator, rowOffset, maxColumnWidths), 'utf8');
//     }
}

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
// }
