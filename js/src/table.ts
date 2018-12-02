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

import { Vector } from './vector';
import { Schema, Field } from './schema';
import { RecordBatch } from './recordbatch';
import { DataType, Row, Struct } from './type';
import { ChunkedVector, Column } from './column';
import { Asyncified, Vector as TVector } from './interfaces';
import { ArrowIPCInput, ArrowIPCSyncInput, ArrowIPCAsyncInput } from './ipc/input';
import { ArrowDataSource } from './ipc/reader';
import { RecordBatchReader } from './ipc/reader/recordbatchreader';
import { AsyncRecordBatchReader } from './ipc/reader/asyncrecordbatchreader';
// import { Col, Predicate } from './predicate';
// import { read, readAsync } from './ipc/reader/arrow';
// import { writeTableBinary } from './ipc/writer/arrow';
// import { PipeIterator } from './util/node';
// import { Vector, DictionaryVector, IntVector, StructVector } from './vector';
// import { ChunkedView } from './vector/chunked';

// export type NextFunc = (idx: number, batch: RecordBatch) => void;
// export type BindFunc = (batch: RecordBatch) => void;

export interface DataFrame<T extends { [key: string]: DataType; } = any> {
    // count(): number;
    // filter(predicate: Predicate): DataFrame<T>;
    // scan(next: NextFunc, bind?: BindFunc): void;
    // countBy(col: (Col|string)): CountByResult;
    [Symbol.iterator](): IterableIterator<Row<T>>;
}

export class Table<T extends { [key: string]: DataType; } = any> implements DataFrame<T> {
    static empty<R extends { [key: string]: DataType; } = any>() { return new Table<R>(new Schema([]), []); }
    static from<R extends { [key: string]: DataType; } = any>(sources?: ArrowIPCSyncInput): Table<R>;
    static from<R extends { [key: string]: DataType; } = any>(sources: ArrowIPCAsyncInput): Promise<Table<R>>;
    static from<R extends { [key: string]: DataType; } = any>(sources?: ArrowIPCInput): Table<R> | Promise<Table<R>> {
        if (sources == null) return Table.empty<R>();
        const source = new ArrowDataSource(sources) as ArrowDataSource | Asyncified<ArrowDataSource>;
        if (source.isSync()) {
            const reader = source.open() as RecordBatchReader<R>;
            return new Table<R>(reader.schema, [...reader]);
        }
        return (async () => {
            const reader = await source.open() as AsyncRecordBatchReader<R>;
            const recordBatches: RecordBatch[] = [];
            for await (let recordBatch of reader) {
                recordBatches.push(recordBatch);
            }
            return new Table<R>(reader.schema, recordBatches);
        })();
    }
    static async fromAsync<R extends { [key: string]: DataType; } = any>(sources: ArrowIPCInput): Promise<Table<R>> {
        return await Table.from<R>(sources as any);
    }
    static fromStruct<R extends { [key: string]: DataType; } = any>(struct: Vector<Struct<R>>) {
        const schema = new Schema(struct.type.children);
        const chunks = (struct instanceof ChunkedVector ? struct.chunks : [struct]) as TVector<Struct<R>>[];
        return new Table<R>(schema, chunks.map((chunk) => new RecordBatch(schema, chunk.data)));
    }

    public readonly schema: Schema;
    public readonly length: number;
    public readonly numCols: number;
    // List of inner RecordBatches
    public readonly batches: RecordBatch<T>[];
    // List of inner Vectors, possibly spanning batches
    protected readonly _columns: Vector<any>[] = [];
    // Union of all inner RecordBatches into one RecordBatch, possibly chunked.
    // If the Table has just one inner RecordBatch, this points to that.
    // If the Table has multiple inner RecordBatches, then this is a Chunked view
    // over the list of RecordBatches. This allows us to delegate the responsibility
    // of indexing, iterating, slicing, and visiting to the Nested/Chunked Data/Views.
    public readonly batchesUnion: Vector<Struct<T>>;

    constructor(batches: RecordBatch<T>[]);
    constructor(...batches: RecordBatch<T>[]);
    constructor(schema: Schema, batches: RecordBatch<T>[]);
    constructor(schema: Schema, ...batches: RecordBatch<T>[]);
    constructor(...args: any[]) {

        let schema: Schema = null!;

        if (args[0] instanceof Schema) {
            schema = args.shift();
        }

        let batches = args.reduce(function flatten(xs: any[], x: any): any[] {
            return Array.isArray(x) ? x.reduce(flatten, xs) : [...xs, x];
        }, []).filter((x: any): x is RecordBatch<T> => x instanceof RecordBatch);

        if (!schema && !(schema = batches[0] && batches[0].schema)) {
            throw new TypeError('Table must be initialized with a Schema or at least one RecordBatch with a Schema');
        }

        this.schema = schema;
        this.batches = batches;
        this.batchesUnion = batches.length == 0
            ? new RecordBatch<T>(schema, 0, [])
            : batches.length === 1 ? batches[0]
            : ChunkedVector.concat<Struct<T>>(...batches) as Vector<Struct<T>>;

        this.length = this.batchesUnion.length;
        this.numCols = this.schema.fields.length;
    }

    public get(index: number): Struct<T>['TValue'] {
        return this.batchesUnion.get(index)!;
    }
    public getColumn<R extends keyof T>(name: R): Vector<T[R]> | null {
        return this.getColumnAt<T[R]>(this.getColumnIndex(name));
    }
    public getColumnAt<T extends DataType = any>(index: number): Vector<T> | null {
        if (index < 0 || index >= this.numCols) {
            return null;
        }
        if (this.batches.length === 1) {
            return this.batches[0].getChildAt<T>(index) as Vector<T> | null;
        }
        return new Column<T>(
            this.schema.fields[index] as Field<T>,
            this.batches.map((b) => b.getChildAt<T>(index)! as Vector<T>));
    }
    public getColumnIndex<R extends keyof T>(name: R) {
        return this.schema.fields.findIndex((f) => f.name === name);
    }
    public [Symbol.iterator]() {
        return this.batchesUnion[Symbol.iterator]() as IterableIterator<Row<T>>;
    }
    // public filter(predicate: Predicate): DataFrame {
    //     return new FilteredDataFrame(this.batches, predicate);
    // }
    // public scan(next: NextFunc, bind?: BindFunc) {
    //     const batches = this.batches, numBatches = batches.length;
    //     for (let batchIndex = -1; ++batchIndex < numBatches;) {
    //         // load batches
    //         const batch = batches[batchIndex];
    //         if (bind) { bind(batch); }
    //         // yield all indices
    //         for (let index = -1, numRows = batch.length; ++index < numRows;) {
    //             next(index, batch);
    //         }
    //     }
    // }
    // public countBy(name: Col | string): CountByResult {
    //     const batches = this.batches, numBatches = batches.length;
    //     const count_by = typeof name === 'string' ? new Col(name) : name;
    //     // Assume that all dictionary batches are deltas, which means that the
    //     // last record batch has the most complete dictionary
    //     count_by.bind(batches[numBatches - 1]);
    //     const vector = count_by.vector as DictionaryVector;
    //     if (!(vector instanceof DictionaryVector)) {
    //         throw new Error('countBy currently only supports dictionary-encoded columns');
    //     }
    //     // TODO: Adjust array byte width based on overall length
    //     // (e.g. if this.length <= 255 use Uint8Array, etc...)
    //     const counts: Uint32Array = new Uint32Array(vector.dictionary.length);
    //     for (let batchIndex = -1; ++batchIndex < numBatches;) {
    //         // load batches
    //         const batch = batches[batchIndex];
    //         // rebind the countBy Col
    //         count_by.bind(batch);
    //         const keys = (count_by.vector as DictionaryVector).indices;
    //         // yield all indices
    //         for (let index = -1, numRows = batch.length; ++index < numRows;) {
    //             let key = keys.get(index);
    //             if (key !== null) { counts[key]++; }
    //         }
    //     }
    //     return new CountByResult(vector.dictionary, IntVector.from(counts));
    // }
    public count(): number {
        return this.length;
    }
    public select(...columnNames: string[]) {
        return new Table(this.batches.map((batch) => batch.select(...columnNames)));
    }
    // public toString(separator?: string) {
    //     let str = '';
    //     for (const row of this.rowsToString(separator)) {
    //         str += row + '\n';
    //     }
    //     return str;
    // }
    // @ts-ignore
    // public serialize(encoding = 'binary', stream = true) {
    //     return writeTableBinary(this, stream);
    // }
    // public rowsToString(separator = ' | '): PipeIterator<string|undefined> {
    //     return new PipeIterator(tableRowsToString(this, separator), 'utf8');
    // }
}

// class FilteredDataFrame<T extends StructData = StructData> implements DataFrame<T> {
//     private predicate: Predicate;
//     private batches: RecordBatch<T>[];
//     constructor (batches: RecordBatch<T>[], predicate: Predicate) {
//         this.batches = batches;
//         this.predicate = predicate;
//     }
//     public scan(next: NextFunc, bind?: BindFunc) {
//         // inlined version of this:
//         // this.parent.scan((idx, columns) => {
//         //     if (this.predicate(idx, columns)) next(idx, columns);
//         // });
//         const batches = this.batches;
//         const numBatches = batches.length;
//         for (let batchIndex = -1; ++batchIndex < numBatches;) {
//             // load batches
//             const batch = batches[batchIndex];
//             // TODO: bind batches lazily
//             // If predicate doesn't match anything in the batch we don't need
//             // to bind the callback
//             if (bind) { bind(batch); }
//             const predicate = this.predicate.bind(batch);
//             // yield all indices
//             for (let index = -1, numRows = batch.length; ++index < numRows;) {
//                 if (predicate(index, batch)) { next(index, batch); }
//             }
//         }
//     }
//     public count(): number {
//         // inlined version of this:
//         // let sum = 0;
//         // this.parent.scan((idx, columns) => {
//         //     if (this.predicate(idx, columns)) ++sum;
//         // });
//         // return sum;
//         let sum = 0;
//         const batches = this.batches;
//         const numBatches = batches.length;
//         for (let batchIndex = -1; ++batchIndex < numBatches;) {
//             // load batches
//             const batch = batches[batchIndex];
//             const predicate = this.predicate.bind(batch);
//             // yield all indices
//             for (let index = -1, numRows = batch.length; ++index < numRows;) {
//                 if (predicate(index, batch)) { ++sum; }
//             }
//         }
//         return sum;
//     }
//     public *[Symbol.iterator](): IterableIterator<Struct<T>['TValue']> {
//         // inlined version of this:
//         // this.parent.scan((idx, columns) => {
//         //     if (this.predicate(idx, columns)) next(idx, columns);
//         // });
//         const batches = this.batches;
//         const numBatches = batches.length;
//         for (let batchIndex = -1; ++batchIndex < numBatches;) {
//             // load batches
//             const batch = batches[batchIndex];
//             // TODO: bind batches lazily
//             // If predicate doesn't match anything in the batch we don't need
//             // to bind the callback
//             const predicate = this.predicate.bind(batch);
//             // yield all indices
//             for (let index = -1, numRows = batch.length; ++index < numRows;) {
//                 if (predicate(index, batch)) { yield batch.get(index) as any; }
//             }
//         }
//     }
//     public filter(predicate: Predicate): DataFrame<T> {
//         return new FilteredDataFrame<T>(
//             this.batches,
//             this.predicate.and(predicate)
//         );
//     }
//     public countBy(name: Col | string): CountByResult {
//         const batches = this.batches, numBatches = batches.length;
//         const count_by = typeof name === 'string' ? new Col(name) : name;
//         // Assume that all dictionary batches are deltas, which means that the
//         // last record batch has the most complete dictionary
//         count_by.bind(batches[numBatches - 1]);
//         const vector = count_by.vector as DictionaryVector;
//         if (!(vector instanceof DictionaryVector)) {
//             throw new Error('countBy currently only supports dictionary-encoded columns');
//         }
//         // TODO: Adjust array byte width based on overall length
//         // (e.g. if this.length <= 255 use Uint8Array, etc...)
//         const counts: Uint32Array = new Uint32Array(vector.dictionary.length);
//         for (let batchIndex = -1; ++batchIndex < numBatches;) {
//             // load batches
//             const batch = batches[batchIndex];
//             const predicate = this.predicate.bind(batch);
//             // rebind the countBy Col
//             count_by.bind(batch);
//             const keys = (count_by.vector as DictionaryVector).indices;
//             // yield all indices
//             for (let index = -1, numRows = batch.length; ++index < numRows;) {
//                 let key = keys.get(index);
//                 if (key !== null && predicate(index, batch)) { counts[key]++; }
//             }
//         }
//         return new CountByResult(vector.dictionary, IntVector.from(counts));
//     }
// }

// export class CountByResult<T extends DataType = DataType> extends Table<{'values': T, 'counts': Int}> {
//     constructor(values: Vector, counts: IntVector) {
//         super(
//             new RecordBatch<{'values': T, 'counts': Int}>(new Schema([
//                 new Field('values', values.type),
//                 new Field('counts', counts.type)
//             ]),
//             counts.length, [values, counts]
//         ));
//     }
//     public toJSON(): Object {
//         const values = this.getColumnAt(0)!;
//         const counts = this.getColumnAt(1)!;
//         const result = {} as { [k: string]: number | null };
//         for (let i = -1; ++i < this.length;) {
//             result[values.get(i)] = counts.get(i);
//         }
//         return result;
//     }
// }

// // function* tableRowsToString(table: Table, separator = ' | ') {
// //     let rowOffset = 0;
// //     let firstValues = [];
// //     let maxColumnWidths: number[] = [];
// //     let iterators: IterableIterator<string>[] = [];
// //     // Gather all the `rowsToString` iterators into a list before iterating,
// //     // so that `maxColumnWidths` is filled with the maxWidth for each column
// //     // across all RecordBatches.
// //     for (const batch of table.batches) {
// //         const iterator = batch.rowsToString(separator, rowOffset, maxColumnWidths);
// //         const { done, value } = iterator.next();
// //         if (!done) {
// //             firstValues.push(value);
// //             iterators.push(iterator);
// //             rowOffset += batch.length;
// //         }
// //     }
// //     for (const iterator of iterators) {
// //         yield firstValues.shift();
// //         yield* iterator;
// //     }
// // }
