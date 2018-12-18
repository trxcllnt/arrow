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

import { Column } from './column';
import { Schema, Field } from './schema';
import { isPromise } from './util/compat';
import { RecordBatch } from './recordbatch';
import { Vector as VType } from './interfaces';
import { RecordBatchReader } from './ipc/reader';
import { DataType, RowLike, Struct } from './type';
import { Vector, ChunkedVector } from './vector/index';
import { RecordBatchFileWriter, RecordBatchStreamWriter } from './ipc/writer';

export interface DataFrame<T extends { [key: string]: DataType; } = any> {
    count(): number;
    filter(predicate: import('./compute/predicate').Predicate): DataFrame<T>;
    countBy(name: import('./compute/predicate').Col | string): import('./compute/dataframe').CountByResult;
    scan(next: import('./compute/dataframe').NextFunc, bind?: import('./compute/dataframe').BindFunc): void;
    [Symbol.iterator](): IterableIterator<RowLike<T>>;
}

export class Table<T extends { [key: string]: DataType; } = any> implements DataFrame<T> {

    /** @nocollapse */
    public static empty<T extends { [key: string]: DataType; } = any>() { return new Table<T>(new Schema([]), []); }

    public static from<T extends { [key: string]: DataType } = any>(): Table<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: RecordBatchReader<T>): Table<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: import('./ipc/reader').FromArg0): Table<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: import('./ipc/reader').FromArg1): Table<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: import('./ipc/reader').FromArg2): Promise<Table<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source: import('./ipc/reader').FromArg3): Promise<Table<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source: import('./ipc/reader').FromArg4): Promise<Table<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source: PromiseLike<RecordBatchReader<T>>): Promise<Table<T>>;
    /** @nocollapse */
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

    /** @nocollapse */
    public static async fromAsync<T extends { [key: string]: DataType; } = any>(source: import('./ipc/reader').FromArgs): Promise<Table<T>> {
        return await Table.from<T>(source as any);
    }

    /** @nocollapse */
    public static fromVectors<T extends { [key: string]: DataType; } = any>(vectors: VType<T[keyof T]>[], names?: (keyof T)[]) {
        return new Table(RecordBatch.from(vectors, names));
    }

    /** @nocollapse */
    public static fromStruct<T extends { [key: string]: DataType; } = any>(struct: Vector<Struct<T>>) {
        const schema = new Schema<T>(struct.type.children);
        const chunks = (struct instanceof ChunkedVector ? struct.chunks : [struct]) as VType<Struct<T>>[];
        return new Table(schema, chunks.map((chunk) => new RecordBatch(schema, chunk.data)));
    }

    protected _schema: Schema;
    protected _length: number;
    protected _numCols: number;
    // List of inner RecordBatches
    protected _batches: RecordBatch<T>[];
    // List of inner Vectors, possibly spanning batches
    protected readonly _columns: Vector<any>[] = [];
    // Union of all inner RecordBatches into one RecordBatch, possibly chunked.
    // If the Table has just one inner RecordBatch, this points to that.
    // If the Table has multiple inner RecordBatches, then this is a Chunked view
    // over the list of RecordBatches. This allows us to delegate the responsibility
    // of indexing, iterating, slicing, and visiting to the Nested/Chunked Data/Views.
    protected _batchesUnion: Vector<Struct<T>>;

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

        this._schema = schema;
        this._batches = batches;
        this._batchesUnion = batches.length == 0
            ? new RecordBatch<T>(schema, 0, [])
            : batches.length === 1 ? batches[0]
            : ChunkedVector.concat<Struct<T>>(...batches) as Vector<Struct<T>>;

        this._length = this.batchesUnion.length;
        this._numCols = this.schema.fields.length;
    }

    public get schema() { return this._schema; }
    public get length() { return this._length; }
    public get numCols() { return this._numCols; }
    public get batches() { return this._batches; }
    public get batchesUnion() { return this._batchesUnion; }

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
}
