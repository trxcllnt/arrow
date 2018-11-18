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
import * as metadata from './metadata/message';
import { ITERATOR_DONE } from '../io/interfaces';
import { OptionallyAsync, Asyncified } from '../interfaces';
import { VectorLoader, JSONVectorLoader } from '../visitor/vectorloader';
import { MessageReader, AsyncMessageReader, JSONMessageReader } from './message';

import {
    ArrowFile, AsyncArrowFile,
    ArrowInput, AsyncArrowInput,
    ArrowStream, AsyncArrowStream,
    ArrowIPCInput, resolveInputFormat,
    InputResolver, AsyncInputResolver, ArrowJSON,
} from './input';

type RecordBatchReaders = RecordBatchJSONReader   |
                          RecordBatchFileReader   | AsyncRecordBatchFileReader   |
                          RecordBatchStreamReader | AsyncRecordBatchStreamReader ;

const JSONReaderImpl = (input: ArrowJSON) => new RecordBatchJSONReader(input);
const FileReaderImpl = (input: ArrowFile | AsyncArrowFile) => input.isSync() ? new RecordBatchFileReader(input) : new AsyncRecordBatchFileReader(input);
const StreamReaderImpl = (input: ArrowStream | AsyncArrowStream) => input.isSync() ? new RecordBatchStreamReader(input) : new AsyncRecordBatchStreamReader(input);
const RecordBatchReaderImpl = (input: AsyncArrowInput | ArrowInput | null): RecordBatchReaders | Promise<RecordBatchReaders> => {
    if (input && input.isJSON()) { return JSONReaderImpl(input).open(); }
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
    protected _dictionaryIndex = 0;
    protected _recordBatchIndex = 0;
    protected _dictionaries: Map<number, Vector>;
    public get schema() { return this._schema; }
    public get dictionaries() { return this._dictionaries; }
    public get numDictionaries() { return this._dictionaryIndex; }
    public get numRecordBatches() { return this._recordBatchIndex; }

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

    public abstract next(value?: any): IteratorResult<RecordBatch> | Promise<IteratorResult<RecordBatch>>;

    public abstract open(): this | Promise<this>;
    public abstract readSchema(): Schema | null | Promise<Schema | null>;
    public abstract readMessage<T extends MessageHeader>(type?: T | null): Message | null | Promise<Message | null>;

    protected _reset(schema?: Schema | null) {
        this._schema = <any> schema;
        this._dictionaryIndex = 0;
        this._recordBatchIndex = 0;
        this._dictionaries = new Map();
        return this;
    }
}

export abstract class RecordBatchReader<T extends { [key: string]: DataType } = any>
       extends AbstractRecordBatchReader<MessageReader>
       implements IterableIterator<RecordBatch<T>> {

    public open() { this.readSchema(); return this; }
    public readSchema() {
        return this._schema || (this._schema = this.source.readSchema());
    }
    public readMessage<T extends MessageHeader>(type?: T | null): Message<T> | null {
        const message = this.source.readMessage(type);
        if (message && message.isDictionaryBatch()) {
            this._dictionaryIndex++;
        } else if (message && message.isRecordBatch()) {
            this._recordBatchIndex++;
        }
        return message;
    }
    public next(): IteratorResult<RecordBatch<T>> {
        let message: Message | null;
        let schema = this.readSchema();
        let dictionaries = this.dictionaries;
        let header: metadata.RecordBatch | metadata.DictionaryBatch;
        while (message = this.readMessage()) {
            if (message.isSchema()) {
                this._reset(message.header()! as Schema<T>);
                break;
            } else if (message.isRecordBatch() && (header = message.header()!)) {
                return { done: false, value: this._loadRecordBatch(schema, message.bodyLength, header) };
            } else if (message.isDictionaryBatch() && (header = message.header()!)) {
                dictionaries.set(header.id, this._loadDictionaryBatch(schema, message.bodyLength, header));
            }
        }
        return ITERATOR_DONE;
    }
    protected _loadRecordBatch(schema: Schema<T>, bodyLength: number, header: metadata.RecordBatch) {
        const loader = this._vectorLoader(bodyLength, header);
        return RecordBatch.new<T>(schema, header.length, loader.visitMany(schema.fields));
    }
    protected _loadDictionaryBatch(schema: Schema<T>, bodyLength: number, header: metadata.DictionaryBatch): Vector {
        const { dictionaries } = this;
        const { id, isDelta } = header;
        if (isDelta || !dictionaries.get(id)) {
            const type = schema.dictionaries.get(id)!;
            const loader = this._vectorLoader(bodyLength, header);
            const vector = isDelta ? dictionaries.get(id)!.concat(
                Vector.new(loader.visit(type))) as any :
                Vector.new(loader.visit(type)) ;
            return (type.dictionaryVector = vector);
        }
        return dictionaries.get(id)!;
    }
    protected _vectorLoader(bodyLength: number, metadata: metadata.RecordBatch | metadata.DictionaryBatch) {
        return new VectorLoader(this.source.readMessageBody(bodyLength), metadata.nodes, metadata.buffers);
    }
}

export abstract class AsyncRecordBatchReader<T extends { [key: string]: DataType } = any>
       extends AbstractRecordBatchReader<AsyncMessageReader>
       implements AsyncIterableIterator<RecordBatch<T>> {

    public async open() { await this.readSchema(); return this; }
    public async readSchema() {
        return this._schema || (this._schema = await this.source.readSchema());
    }
    public async readMessage<T extends MessageHeader>(type?: T | null): Promise<Message<T> | null> {
        const message = await this.source.readMessage(type);
        if (message && message.isDictionaryBatch()) {
            this._dictionaryIndex++;
        } else if (message && message.isRecordBatch()) {
            this._recordBatchIndex++;
        }
        return message;
    }
    public async next(): Promise<IteratorResult<RecordBatch<T>>> {
        let message: Message | null;
        let schema = await this.readSchema();
        let dictionaries = this.dictionaries;
        let header: metadata.RecordBatch | metadata.DictionaryBatch;
        while (message = await this.readMessage()) {
            if (message.isSchema()) {
                this._reset(message.header()! as Schema<T>);
                break;
            } else if (message.isRecordBatch() && (header = message.header()!)) {
                return { done: false, value: await this._loadRecordBatch(schema, message.bodyLength, header) };
            } else if (message.isDictionaryBatch() && (header = message.header()!)) {
                dictionaries.set(header.id, await this._loadDictionaryBatch(schema, message.bodyLength, header));
            }
        }
        return ITERATOR_DONE;
    }
    protected async _loadRecordBatch(schema: Schema<T>, bodyLength: number, header: metadata.RecordBatch) {
        const loader = await this._vectorLoader(bodyLength, header);
        return RecordBatch.new<T>(schema, header.length, loader.visitMany(schema.fields));
    }
    protected async _loadDictionaryBatch(schema: Schema<T>, bodyLength: number, header: metadata.DictionaryBatch): Promise<Vector> {
        const { dictionaries } = this;
        const { id, isDelta } = header;
        if (isDelta || !dictionaries.get(id)) {
            const type = schema.dictionaries.get(id)!;
            const loader = await this._vectorLoader(bodyLength, header);
            const vector = isDelta ? dictionaries.get(id)!.concat(
                Vector.new(loader.visit(type))) as any :
                Vector.new(loader.visit(type)) ;
            return (type.dictionaryVector = vector);
        }
        return dictionaries.get(id)!;
    }
    protected async _vectorLoader(bodyLength: number, metadata: metadata.RecordBatch | metadata.DictionaryBatch) {
        return new VectorLoader(await this.source.readMessageBody(bodyLength), metadata.nodes, metadata.buffers);
    }
}

export class RecordBatchJSONReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {
    // @ts-ignore
    protected source: JSONMessageReader;
    constructor(json: ArrowJSON) { super(new JSONMessageReader(json)); }
    protected _vectorLoader(bodyLength: number, metadata: metadata.RecordBatch | metadata.DictionaryBatch) {
        return new JSONVectorLoader(this.source.readMessageBody(bodyLength), metadata.nodes, metadata.buffers);
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
    public readMessage<T extends MessageHeader>(type?: T | null): Message<T> | null {
        const block =
              (this._dictionaryIndex < this.numDictionaries) ? this._footer.getDictionaryBatch(this._dictionaryIndex)
            : (this._recordBatchIndex < this.numRecordBatches) ? this._footer.getRecordBatch(this._recordBatchIndex)
            : null;
        return !(block && this.file.seek(block.offset)) ? null : super.readMessage(type);
    }
    public readRecordBatch(index: number) {
        const block = this._footer.getRecordBatch(index);
        if (block && this.file.seek(block.offset)) {
            const message = this.readMessage(MessageHeader.RecordBatch);
            if (message && message.isRecordBatch()) {
                return this._loadRecordBatch(this.schema, message.bodyLength, message.header()!);
            }
        }
        return null;
    }
    public readDictionaryBatch(index: number) {
        const block = this._footer.getDictionaryBatch(index);
        if (block && this.file.seek(block.offset)) {
            const message = this.readMessage(MessageHeader.DictionaryBatch);
            if (message && message.isDictionaryBatch()) {
                return this._loadDictionaryBatch(this.schema, message.bodyLength, message.header()!);
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
            for (const block of footer.dictionaryBatches()) {
                block && this.readDictionaryBatch(this._dictionaryIndex);
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
    public async readMessage<T extends MessageHeader>(type?: T | null): Promise<Message<T> | null> {
        const block =
              (this._dictionaryIndex < this.numDictionaries) ? this._footer.getDictionaryBatch(this._dictionaryIndex)
            : (this._recordBatchIndex < this.numRecordBatches) ? this._footer.getRecordBatch(this._recordBatchIndex)
            : null;
        return !(block && (await this.file.seek(block.offset))) ? null : await super.readMessage(type);
    }
    public async readRecordBatch(index: number) {
        const block = this._footer.getRecordBatch(index);
        if (block && (await this.file.seek(block.offset))) {
            const message = await super.readMessage(MessageHeader.RecordBatch);
            if (message && message.isRecordBatch()) {
                return await this._loadRecordBatch(this.schema, message.bodyLength, message.header()!);
            }
        }
        return null;
    }
    public async readDictionaryBatch(index: number) {
        const block = this._footer.getDictionaryBatch(index);
        if (block && (await this.file.seek(block.offset))) {
            const message = await super.readMessage(MessageHeader.DictionaryBatch);
            if (message && message.isDictionaryBatch()) {
                return await this._loadDictionaryBatch(this.schema, message.bodyLength, message.header()!);
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
            for (const block of footer.dictionaryBatches()) {
                block && (await this.readDictionaryBatch(this._dictionaryIndex));
            }
        }
        return this._footer;
    }
}
