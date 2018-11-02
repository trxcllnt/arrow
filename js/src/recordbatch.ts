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
import { Vector } from './vector';
import { DataType, Struct } from './type';
// import { PipeIterator } from './util/node';
// import { valueToString, leftPad } from './util/pretty';

export class RecordBatch<T extends { [key: string]: DataType } = any> {

//     public static from<T extends { [key: string]: DataType } = any>(vectors: Vector[]) {
//         const numRows = Math.max(...vectors.map((v) => v.length));
//         return new RecordBatch<T>(Schema.from(vectors), numRows, vectors);
//     }

    static new<T extends { [key: string]: DataType } = any>(schema: Schema, numRows: number, columns: (Data | Vector)[]) {
        const childData = columns.map((x) => x instanceof Vector ? x.data : x);
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
    public clone<R extends { [key: string]: DataType } = any>(data: Data<Struct<R>>, children = this._children) {
        return new RecordBatch(this.schema, data as any, children as any);
    }
//     public select(...columnNames: string[]) {
//         const fields = this.schema.fields;
//         const namesToKeep = columnNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
//         return new RecordBatch(
//             this.schema.select(...columnNames), this.length,
//             this.childData.filter((_, i) => namesToKeep[fields[i].name])
//         );
//     }
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
