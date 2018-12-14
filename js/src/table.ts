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
import { isPromise } from './util/compat';
import { RecordBatch } from './recordbatch';
import { Vector as VType } from './interfaces';
import { ChunkedVector, Column } from './column';
import { DataType, RowLike, Struct } from './type';
import { RecordBatchFileWriter, RecordBatchStreamWriter } from './ipc/writer';
import {
    RecordBatchReader,
    FromArg0, FromArg1, FromArg2, FromArg3, FromArg4, FromArgs
} from './ipc/reader';

export interface DataFrame<T extends { [key: string]: DataType; } = any> {
    count(): number;
    filter(predicate: Predicate): DataFrame<T>;
    scan(next: NextFunc, bind?: BindFunc): void;
    countBy<K extends keyof T>(name: Col | K): CountByResult;
    [Symbol.iterator](): IterableIterator<RowLike<T>>;
}

export class Table<T extends { [key: string]: DataType; } = any> implements DataFrame<T> {

    public static empty<T extends { [key: string]: DataType; } = any>() { return new Table<T>(new Schema([]), []); }

    public static from<T extends { [key: string]: DataType } = any>(): Table<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg0): Table<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg1): Table<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: RecordBatchReader<T>): Table<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg2): Promise<Table<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg3): Promise<Table<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg4): Promise<Table<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source: PromiseLike<RecordBatchReader<T>>): Promise<Table<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source?: any) {

        if (!source) { return Table.empty<T>(); }

        let reader = RecordBatchReader.from<T>(source) as RecordBatchReader<T> | Promise<RecordBatchReader<T>>;

        if (isPromise<RecordBatchReader<T>>(reader)) {
            return (async () => await Table.from(await reader))();
        }
        if (reader.isSync() && (reader = reader.open())) {
            return !reader.schema ? Table.empty<T>() : new Table<T>(reader.schema, [...reader]);
        }
        return (async (opening) => {
            const reader = await opening;
            const schema = reader.schema;
            const batches: RecordBatch[] = [];
            if (schema) {
                for await (let batch of reader) {
                    batches.push(batch);
                }
                return new Table<T>(schema, batches);
            }
            return Table.empty<T>();
        })(reader.open());
    }

    static async fromAsync<T extends { [key: string]: DataType; } = any>(source: FromArgs): Promise<Table<T>> {
        return await Table.from<T>(source as any);
    }
    static fromVectors<T extends { [key: string]: DataType; } = any>(vectors: VType<T[keyof T]>[], names?: (keyof T)[]) {
        return new Table(RecordBatch.from(vectors, names));
     }
     static fromStruct<T extends { [key: string]: DataType; } = any>(struct: Vector<Struct<T>>) {
        const schema = new Schema<T>(struct.type.children);
        const chunks = (struct instanceof ChunkedVector ? struct.chunks : [struct]) as VType<Struct<T>>[];
        return new Table(schema, chunks.map((chunk) => new RecordBatch(schema, chunk.data)));
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
        return this.getColumnAt(this.getColumnIndex(name)) as Vector<T[R]> | null;
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
        return this.batchesUnion[Symbol.iterator]() as IterableIterator<RowLike<T>>;
    }
    // @ts-ignore
    public serialize(encoding = 'binary', stream = true) {
        const writer = !stream
            ? RecordBatchFileWriter
            : RecordBatchStreamWriter;
        return writer.writeAll(this.batches).toUint8Array(true);
    }
    public count(): number {
        return this.length;
    }
    public select(...columnNames: string[]) {
        return new Table(this.batches.map((batch) => batch.select(...columnNames)));
    }
    public scan(next: NextFunc, bind?: BindFunc) {
        return new Dataframe(this.batches).scan(next, bind);
    }
    public filter(predicate: Predicate): DataFrame {
        return new Dataframe(this.batches).filter(predicate);
    }
    public countBy<K extends keyof T>(name: Col | K) {
        return new Dataframe(this.batches).countBy(name);
    }
    // public toString(separator?: string) {
    //     let str = '';
    //     for (const row of this.rowsToString(separator)) {
    //         str += row + '\n';
    //     }
    //     return str;
    // }
    // public rowsToString(separator = ' | '): PipeIterator<string|undefined> {
    //     return new PipeIterator(tableRowsToString(this, separator), 'utf8');
    // }
}

// protect batches, batchesUnion from es2015/umd mangler
(<any> Table.prototype).batches = Object.freeze([]);
(<any> Table.prototype).batchesUnion = Object.freeze([]);

import { Predicate, Col } from './compute/predicate';
import { NextFunc, BindFunc, CountByResult, Dataframe } from './compute/dataframe';

// function* tableRowsToString(table: Table, separator = ' | ') {
//     let rowOffset = 0;
//     let firstValues = [];
//     let maxColumnWidths: number[] = [];
//     let iterators: IterableIterator<string>[] = [];
//     // Gather all the `rowsToString` iterators into a list before iterating,
//     // so that `maxColumnWidths` is filled with the maxWidth for each column
//     // across all RecordBatches.
//     for (const batch of table.batches) {
//         const iterator = batch.rowsToString(separator, rowOffset, maxColumnWidths);
//         const { done, value } = iterator.next();
//         if (!done) {
//             firstValues.push(value);
//             iterators.push(iterator);
//             rowOffset += batch.length;
//         }
//     }
//     for (const iterator of iterators) {
//         yield firstValues.shift();
//         yield* iterator;
//     }
// }
