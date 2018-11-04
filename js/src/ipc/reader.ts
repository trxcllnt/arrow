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

import { DataType } from '../type';
import { Vector } from '../vector';
import { Schema } from '../schema';
import { MessageHeader } from '../enum';
import { Footer } from './metadata/file';
import { magicAndPadding } from './magic';
import { Message } from './metadata/message';
import { RecordBatch } from '../recordbatch';
import { ITERATOR_DONE } from '../io/interfaces';
import { OptionallyAsync, Asyncified } from '../interfaces';
import { MessageReader, AsyncMessageReader } from './message';
import { VectorLoader } from '../visitor/vectordataloader';

import {
    ArrowFile, AsyncArrowFile,
    ArrowInput, AsyncArrowInput,
    ArrowStream, AsyncArrowStream,
    ArrowIPCInput, resolveInputFormat,
    InputResolver, AsyncInputResolver,
} from './input';

type RecordBatchReaders = RecordBatchFileReader   | AsyncRecordBatchFileReader   |
                          RecordBatchStreamReader | AsyncRecordBatchStreamReader ;

const FileReaderImpl = (input: ArrowFile | AsyncArrowFile) => input.isSync() ? new RecordBatchFileReader(input) : new AsyncRecordBatchFileReader(input);
const StreamReaderImpl = (input: ArrowStream | AsyncArrowStream) => input.isSync() ? new RecordBatchStreamReader(input) : new AsyncRecordBatchStreamReader(input);
const RecordBatchReaderImpl = (input: AsyncArrowInput | ArrowInput | null): RecordBatchReaders | Promise<RecordBatchReaders> => {
    if (input && input.isFile()) { return FileReaderImpl(input).open(); }
    if (input && input.isStream()) { return StreamReaderImpl(input).open(); }
    return new RecordBatchStreamReader(new ArrowStream(function*(): any {}()));
};

export class ArrowDataSource implements OptionallyAsync<ArrowDataSource> {
    private resolver: InputResolver | AsyncInputResolver;
    constructor(source: ArrowIPCInput) {
        this.resolver = resolveInputFormat(source);
    }
    isSync(): this is ArrowDataSource { return this.resolver.isSync(); }
    isAsync(): this is Asyncified<ArrowDataSource> { return this.resolver.isAsync(); }
    open(this: ArrowDataSource): RecordBatchReaders;
    open(this: Asyncified<ArrowDataSource>): Promise<RecordBatchReaders>;
    open() {
        const resolver = this.resolver;
        return resolver.isSync()
            ? RecordBatchReaderImpl(resolver.resolve())
            : resolver.resolve().then(RecordBatchReaderImpl);
    }
}

abstract class AbstractRecordBatchReader<TSource extends MessageReader | AsyncMessageReader> {

    // @ts-ignore
    protected _schema: Schema;
    // @ts-ignore
    protected _footer: Footer;
    protected _numDictionaries = 0;
    protected _numRecordBatches = 0;
    protected _recordBatchIndex = 0;
    protected _dictionaries: Map<number, Vector>;
    public get schema() { return this._schema; }
    public get dictionaries() { return this._dictionaries; }
    public get numDictionaries() { return this._numDictionaries; }
    public get numRecordBatches() { return this._numRecordBatches; }

    constructor(protected source: TSource, dictionaries = new Map<number, Vector>()) {
        this._dictionaries = dictionaries;
    }

    public [Symbol.iterator]() { return this; }
    public [Symbol.asyncIterator]() { return this; }
    public    throw(value?: any) { return this.source.throw(value) as ReturnType<TSource['throw']>;      }
    public   return(value?: any) { return this.source.return(value) as ReturnType<TSource['return']>;    }
    public   isSync(): this is RecordBatchReader       { return this instanceof RecordBatchReader;       }
    public   isFile(): this is RecordBatchFileReader   { return this instanceof RecordBatchFileReader;   }
    public  isAsync(): this is AsyncRecordBatchReader  { return this instanceof AsyncRecordBatchReader;  }
    public isStream(): this is RecordBatchStreamReader { return this instanceof RecordBatchStreamReader; }
    protected _reset(schema?: Schema | null) {
        this._schema = <any> schema;
        this._numDictionaries = 0;
        this._numRecordBatches = 0;
        this._recordBatchIndex = 0;
        this._dictionaries = new Map();
        return this;
    }

    public abstract open(): this | Promise<this>;
    public abstract read(): Message | null | Promise<Message | null>;
    public abstract readSchema(): Schema | null | Promise<Schema | null>;
    public abstract next(value?: any): IteratorResult<RecordBatch> | Promise<IteratorResult<RecordBatch>>;
}

export abstract class RecordBatchReader<T extends { [key: string]: DataType } = any>
       extends AbstractRecordBatchReader<MessageReader>
       implements IterableIterator<RecordBatch<T>> {

    public open() { this.readSchema(); return this; }
    public readSchema() {
        return this._schema || (this._schema = this.source.readSchema());
    }
    public read(): Message | null {
        const message = this.source.next().value;
        if (message && message.isDictionaryBatch()) {
            this._numDictionaries++;
        } else if (message && message.isRecordBatch()) {
            this._numRecordBatches++;
            this._recordBatchIndex++;
        }
        return message;
    }
    public next(): IteratorResult<RecordBatch<T>> {
        let message: Message | null, schema = this._schema;
        while (message = this.read()) {
            if (message.isSchema()) {
                this._reset(message.header()! as Schema<T>);
                break;
            } else if (message.isRecordBatch()) {
                return { done: false, value: readRecordBatch<T>(this, schema, message) };
            } else if (message.isDictionaryBatch()) {
                readDictionaryBatch(this, schema, message);
            }
        }
        return ITERATOR_DONE;
    }
}

export abstract class AsyncRecordBatchReader<T extends { [key: string]: DataType } = any>
       extends AbstractRecordBatchReader<AsyncMessageReader>
       implements AsyncIterableIterator<RecordBatch<T>> {

    public async open() { await this.readSchema(); return this; }
    public async readSchema() {
        return this._schema || (this._schema = await this.source.readSchema());
    }
    public async read(): Promise<Message | null> {
        const message = (await this.source.next()).value;
        if (message && message.isDictionaryBatch()) {
            this._numDictionaries++;
        } else if (message && message.isRecordBatch()) {
            this._recordBatchIndex++;
            this._numRecordBatches++;
        }
        return message;
    }
    public async next(): Promise<IteratorResult<RecordBatch<T>>> {
        let message: Message | null, schema = this._schema;
        while (message = await this.read()) {
            if (message.isSchema()) {
                this._reset(message.header()! as Schema<T>);
                break;
            } else if (message.isRecordBatch()) {
                return { done: false, value: readRecordBatch(this, schema, message) };
            } else if (message.isDictionaryBatch()) {
                readDictionaryBatch(this, schema, message);
            }
        }
        return ITERATOR_DONE;
    }
}

export class RecordBatchStreamReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {
    constructor(input: ArrowStream) { super(new MessageReader(input)); }
}

export class AsyncRecordBatchStreamReader<T extends { [key: string]: DataType } = any> extends AsyncRecordBatchReader<T> {
    constructor(input: AsyncArrowStream) { super(new AsyncMessageReader(input)); }
}

export class RecordBatchFileReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {
    public get footer() { return this._footer; }
    public get numDictionaries() { return this._footer.numDictionaries; }
    public get numRecordBatches() { return this._footer.numRecordBatches; }
    constructor(protected file: ArrowFile) { super(new MessageReader(file)); }
    public readSchema() {
        return this._schema || (this._schema = this.readFooter().schema);
    }
    public read() {
        const block = this._footer.getRecordBatch(this._recordBatchIndex);
        if (block && this.file.seek(block.offset)) {
            return super.read();
        }
        return null; 
    }
    public readRecordBatch(index: number) {
        const block = this._footer.getRecordBatch(index);
        if (block && this.file.seek(block.offset)) {
            const message = this.source.readMessage(MessageHeader.RecordBatch);
            if (message && message.isRecordBatch()) {
                return readRecordBatch(this, this._schema, message);
            }
        }
        return null;
    }
    public readFooter() {
        if (!this._footer) {
            const { file } = this;
            const size = file.size;
            const offset = size - magicAndPadding;
            const length = file.readInt32(offset);
            const buffer = file.readAt(offset - length, length);
            const footer = this._footer = Footer.decode(buffer);
            for (const { offset } of footer.dictionaryBatches()) {
                file.seek(offset) && this.read();
            }
        }
        return this._footer;
    }
}

export class AsyncRecordBatchFileReader<T extends { [key: string]: DataType } = any> extends AsyncRecordBatchReader<T> {
    public get footer() { return this._footer; }
    public get numDictionaries() { return this._footer.numDictionaries; }
    public get numRecordBatches() { return this._footer.numRecordBatches; }
    constructor(protected file: AsyncArrowFile) { super(new AsyncMessageReader(file)); }
    public async readSchema() {
        return this._schema || (this._schema = (await this.readFooter()).schema);
    }
    public async read(): Promise<Message | null> {
        const block = this._footer.getRecordBatch(this._recordBatchIndex);
        if (block && (await this.file.seek(block.offset))) {
            return await super.read();
        }
        return null; 
    }
    public async readRecordBatch(index: number) {
        const block = this._footer.getRecordBatch(index);
        if (block && (await this.file.seek(block.offset))) {
            const message = await this.source.readMessage(MessageHeader.RecordBatch);
            if (message && message.isRecordBatch()) {
                return readRecordBatch(this, this._schema, message);
            }
        }
        return null;
    }
    public async readFooter() {
        if (!this._footer) {
            const { file } = this;
            const offset = file.size - magicAndPadding;
            const length = await file.readInt32(offset);
            const buffer = await file.readAt(offset - length, length);
            const footer = this._footer = Footer.decode(buffer);
            for (const { offset } of footer.dictionaryBatches()) {
                (await file.seek(offset)) && (await this.read());
            }
        }
        return this._footer;
    }
}

function readRecordBatch<T extends { [key: string]: DataType } = any>(
    reader: RecordBatchReader<T> | AsyncRecordBatchReader<T>,
    schema: Schema,
    message: Message<MessageHeader.RecordBatch>
) {
    const { body } = message, { dictionaries } = reader;
    const { nodes, buffers, length } = message.header()!;
    const loader = new VectorLoader(body, nodes, buffers, dictionaries);
    return RecordBatch.new<T>(schema, length, loader.visitMany(schema.fields));
}

function readDictionaryBatch(
    reader: RecordBatchReader | AsyncRecordBatchReader,
    schema: Schema,
    message: Message<MessageHeader.DictionaryBatch>
) {
    const { body } = message, { dictionaries } = reader;
    const { id, nodes, buffers, isDelta } = message.header()!;
    const loader = new VectorLoader(body, nodes, buffers, dictionaries);
    let vector = Vector.new(loader.visit(schema.dictionaries.get(id)!));
    if (isDelta && dictionaries.has(id)) {
        vector = dictionaries.get(id)!.concat(vector) as any;
    }
    dictionaries.set(id, vector);
}
