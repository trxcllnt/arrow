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
import { MessageHeader } from '../enum';
import { Footer } from './metadata/file';
import { Schema, Field } from '../schema';
import streamAdapters from '../io/adapters';
import { Message } from './metadata/message';
import { RecordBatch } from '../recordbatch';
import * as metadata from './metadata/message';
import { ArrayBufferViewInput, toUint8Array } from '../util/buffer';
import { RandomAccessFile, AsyncRandomAccessFile } from '../io/file';
import { VectorLoader, JSONVectorLoader } from '../visitor/vectorloader';
import { ReadableByteStream, AsyncReadableByteStream } from '../io/stream';
import { ArrowJSON, ArrowJSONLike, FileHandle, Streamable, ITERATOR_DONE } from '../io/interfaces';
import { isPromise, isArrowJSON, isFileHandle, isFetchResponse, isAsyncIterable, isReadableDOMStream, isReadableNodeStream } from '../util/compat';
import { MessageReader, AsyncMessageReader, checkForMagicArrowString, magicLength, magicAndPadding, magicX2AndPadding, JSONMessageReader } from './message';

export type FromArg0 = ArrowJSONLike;
export type FromArg1 = Iterable<ArrayBufferViewInput> | ArrayBufferViewInput;
export type FromArg2 = PromiseLike<Iterable<ArrayBufferViewInput> | ArrayBufferViewInput>;
export type FromArg3 = NodeJS.ReadableStream | ReadableStream<ArrayBufferViewInput> | AsyncIterable<ArrayBufferViewInput>;
export type FromArg4 = Response | FileHandle | PromiseLike<FileHandle> | PromiseLike<Response>;
export type FromArgs = FromArg0 | FromArg3 | FromArg1 | FromArg2 | FromArg4;

export abstract class RecordBatchReader<T extends { [key: string]: DataType } = any> extends Streamable<RecordBatch<T>> {

    protected constructor(protected impl: IRecordBatchReaderImpl<T>) { super(); }

    public get closed() { return this.impl.closed; }
    public get schema() { return this.impl.schema; }
    public get autoClose() { return this.impl.autoClose; }
    public get dictionaries() { return this.impl.dictionaries; }
    public get numDictionaries() { return this.impl.numDictionaries; }
    public get numRecordBatches() { return this.impl.numRecordBatches; }

    public next(value?: any) { return this.impl.next(value); }
    public throw(value?: any) { return this.impl.throw(value); }
    public return(value?: any) { return this.impl.return(value); }
    public reset(schema?: Schema<T> | null) { this.impl.reset(schema); return this; }

    public abstract open(autoClose?: boolean): this | Promise<this>;
    public abstract close(): this | Promise<this>;
    public abstract [Symbol.iterator](): IterableIterator<RecordBatch<T>>;
    public abstract [Symbol.asyncIterator](): AsyncIterableIterator<RecordBatch<T>>;

    public asReadableDOMStream() { return streamAdapters.toReadableDOMStream(this); }
    public asReadableNodeStream() { return streamAdapters.toReadableNodeStream(this, { objectMode: true }); }

    public isSync(): this is RecordBatchFileReader<T> | RecordBatchStreamReader<T> { return this.impl.isSync(); }
    public isFile(): this is RecordBatchFileReader<T> | AsyncRecordBatchFileReader<T> { return this.impl.isFile(); }
    public isStream(): this is RecordBatchStreamReader<T> | AsyncRecordBatchStreamReader<T> { return this.impl.isStream(); }
    public isAsync(): this is AsyncRecordBatchFileReader<T> | AsyncRecordBatchStreamReader<T> { return this.impl.isAsync(); }

    public static asNodeStream(): import('stream').Duplex { throw new Error(`"asNodeStream" not available in this environment`); }
    public static asDOMStream<T extends { [key: string]: DataType }>(): { writable: WritableStream<Uint8Array>, readable: ReadableStream<RecordBatch<T>> } {
        throw new Error(`"asDOMStream" not available in this environment`);
    }

    public static from<T extends RecordBatchReader>(source: T): T;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg0): RecordBatchStreamReader<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg1): RecordBatchFileReader<T> | RecordBatchStreamReader<T>;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg2): Promise<RecordBatchFileReader<T> | RecordBatchStreamReader<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg3): Promise<RecordBatchFileReader<T> | AsyncRecordBatchStreamReader<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source: FromArg4): Promise<AsyncRecordBatchFileReader<T> | AsyncRecordBatchStreamReader<T>>;
    public static from<T extends { [key: string]: DataType } = any>(source: any) {
        if (source instanceof RecordBatchReader) {
            return source;
        } else if (isArrowJSON(source)) {
            return RecordBatchReader.fromJSON<T>(source);
        } else if (isFileHandle(source)) {
            return RecordBatchReader.fromFileHandle<T>(source);
        } else if (isPromise<FromArg1>(source)) {
            return (async () => await RecordBatchReader.from<T>(await source))();
        } else if (isPromise<FileHandle | Response>(source)) {
            return (async () => await RecordBatchReader.from<T>(await source))();
        } else if (isFetchResponse(source) && (source = source.body)) {
            return RecordBatchReader.fromAsyncByteStream<T>(new AsyncReadableByteStream(source));
        } else if (isReadableNodeStream(source) || isReadableDOMStream(source) || isAsyncIterable(source)) {
            return RecordBatchReader.fromAsyncByteStream<T>(new AsyncReadableByteStream(source));
        }
        return RecordBatchReader.fromByteStream<T>(new ReadableByteStream(source));
    }

    private static fromJSON<T extends { [key: string]: DataType }>(source: ArrowJSONLike) {
        return new RecordBatchStreamReader<T>(new ArrowJSON(source));
    }
    private static fromByteStream<T extends { [key: string]: DataType }>(source: ReadableByteStream) {
        const bytes = source.peek((magicLength + 7) & ~7);
        return bytes && bytes.byteLength >= 4
            ? checkForMagicArrowString(bytes)
            ? new RecordBatchFileReader<T>(source.read())
            : new RecordBatchStreamReader<T>(source)
            : new RecordBatchStreamReader<T>(function*(): any {}());
    }
    private static async fromAsyncByteStream<T extends { [key: string]: DataType }>(source: AsyncReadableByteStream) {
        const bytes = await source.peek((magicLength + 7) & ~7);
        return bytes && bytes.byteLength >= 4
            ? checkForMagicArrowString(bytes)
            ? new RecordBatchFileReader<T>(await source.read())
            : new AsyncRecordBatchStreamReader<T>(source)
            : new AsyncRecordBatchStreamReader<T>(async function*(): any {}());
    }
    private static async fromFileHandle<T extends { [key: string]: DataType }>(source: FileHandle) {
        const { size } = await source.stat();
        const file = new AsyncRandomAccessFile(source, size);
        if (size >= magicX2AndPadding) {
            if (checkForMagicArrowString(await file.readAt(0, (magicLength + 7) & ~7))) {
                return new AsyncRecordBatchFileReader<T>(file);
            }
        }
        return new AsyncRecordBatchStreamReader<T>(file);
    }
}

export class RecordBatchFileReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {
    // @ts-ignore
    protected impl: RecordBatchFileReaderImpl<T>;
    constructor(source: AsyncRecordBatchFileReaderImpl<T>);
    constructor(source: RandomAccessFile, dictionaries?: Map<number, Vector>);
    constructor(source: ArrayBufferViewInput, dictionaries?: Map<number, Vector>);
    constructor(source: AsyncRecordBatchFileReaderImpl<T> | RandomAccessFile | ArrayBufferViewInput, dictionaries?: Map<number, Vector>) {
        if (source instanceof AsyncRecordBatchFileReaderImpl) {
            super(source);
        } else if (source instanceof RandomAccessFile) {
            super(new RecordBatchFileReaderImpl(source, dictionaries));
        } else {
            super(new RecordBatchFileReaderImpl(new RandomAccessFile(toUint8Array(source)), dictionaries));
        }
    }
    public get footer() { return this.impl.footer; }
    public close() { this.impl.close(); return this; }
    public open(autoClose?: boolean) { this.impl.open(autoClose); return this; }
    public readRecordBatch(index: number) { return this.impl.readRecordBatch(index); }
    public [Symbol.iterator]() { return (this.impl as IterableIterator<RecordBatch<T>>)[Symbol.iterator](); }
    public async *[Symbol.asyncIterator](): AsyncIterableIterator<RecordBatch<T>> { yield* this[Symbol.iterator](); }
}

export class RecordBatchStreamReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {
    // @ts-ignore
    protected impl: RecordBatchStreamReaderImpl<T>;
    constructor(source: ReadableByteStream | ArrowJSON | ArrayBufferView | Iterable<ArrayBufferView>, dictionaries?: Map<number, Vector>) {
        super(isArrowJSON(source)
            ? new RecordBatchJSONReaderImpl(new JSONMessageReader(source), dictionaries)
            : new RecordBatchStreamReaderImpl(new MessageReader(source), dictionaries));
    }
    public close() { this.impl.close(); return this; }
    public open(autoClose?: boolean) { this.impl.open(autoClose); return this; }
    public [Symbol.iterator]() { return (this.impl as IterableIterator<RecordBatch<T>>)[Symbol.iterator](); }
    public async *[Symbol.asyncIterator](): AsyncIterableIterator<RecordBatch<T>> { yield* this[Symbol.iterator](); }
}

export class AsyncRecordBatchStreamReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {
    // @ts-ignore
    protected impl: AsyncRecordBatchStreamReaderImpl<T>;
    constructor(source: AsyncReadableByteStream | FileHandle | NodeJS.ReadableStream | ReadableStream<ArrayBufferView> | AsyncIterable<ArrayBufferView>, byteLength?: number) {
        super(new AsyncRecordBatchStreamReaderImpl(new AsyncMessageReader(source as FileHandle, byteLength)));
    }
    public async close() { await this.impl.close(); return this; }
    public async open(autoClose?: boolean) { await this.impl.open(autoClose); return this; }
    public [Symbol.asyncIterator]() { return (this.impl as AsyncIterableIterator<RecordBatch<T>>)[Symbol.asyncIterator](); }
    public [Symbol.iterator](): IterableIterator<RecordBatch<T>> { throw new Error(`AsyncRecordBatchStreamReader is not Iterable`); }
}

export class AsyncRecordBatchFileReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {
    // @ts-ignore
    protected impl: AsyncRecordBatchFileReaderImpl<T>;
    constructor(source: AsyncRandomAccessFile);
    constructor(source: AsyncRandomAccessFile, dictionaries: Map<number, Vector>);
    constructor(source: FileHandle, byteLength: number, dictionaries: Map<number, Vector>);
    constructor(source: AsyncRandomAccessFile | FileHandle, ...rest: (number | Map<number, Vector>)[]) {
        let [byteLength, dictionaries] = rest as [number, Map<number, Vector>];
        if (byteLength && typeof byteLength !== 'number') { dictionaries = byteLength; }
        let file = source instanceof AsyncRandomAccessFile ? source : new AsyncRandomAccessFile(source, byteLength);
        super(new AsyncRecordBatchFileReaderImpl(file, dictionaries));
    }
    public get footer() { return this.impl.footer; }
    public async close() { await this.impl.close(); return this; }
    public async open(autoClose?: boolean) { await this.impl.open(autoClose); return this; }
    public readRecordBatch(index: number) { return this.impl.readRecordBatch(index); }
    public [Symbol.asyncIterator]() { return (this.impl as AsyncIterableIterator<RecordBatch<T>>)[Symbol.asyncIterator](); }
    public [Symbol.iterator](): IterableIterator<RecordBatch<T>> { throw new Error(`AsyncRecordBatchFileReader is not Iterable`); }
}

abstract class RecordBatchReaderImplBase<T extends { [key: string]: DataType } = any> {

    // @ts-ignore
    public schema: Schema;
    public closed = false;
    public autoClose = true;
    public dictionaryIndex = 0;
    public recordBatchIndex = 0;
    public dictionaries: Map<number, Vector>;
    public get numDictionaries() { return this.dictionaryIndex; }
    public get numRecordBatches() { return this.recordBatchIndex; }

    constructor(dictionaries = new Map<number, Vector>()) {
        this.dictionaries = dictionaries;
    }
    public isSync(): this is (RecordBatchFileReaderImpl<T> | RecordBatchStreamReaderImpl<T>) {
        return (this instanceof RecordBatchFileReaderImpl) || (this instanceof RecordBatchStreamReaderImpl);
    }
    public isFile(): this is (RecordBatchFileReaderImpl<T> | AsyncRecordBatchFileReaderImpl<T>) {
        return (this instanceof RecordBatchFileReaderImpl) || (this instanceof AsyncRecordBatchFileReaderImpl);
    }
    public isStream(): this is (RecordBatchStreamReaderImpl<T> | AsyncRecordBatchStreamReaderImpl<T>) {
        return (this instanceof RecordBatchStreamReaderImpl) || (this instanceof AsyncRecordBatchStreamReaderImpl);
    }
    public isAsync(): this is (AsyncRecordBatchFileReaderImpl<T> | AsyncRecordBatchStreamReaderImpl<T>) {
        return (this instanceof AsyncRecordBatchFileReaderImpl) || (this instanceof AsyncRecordBatchStreamReaderImpl);
    }
    public reset(schema?: Schema<T> | null) {
        this.dictionaryIndex = 0;
        this.recordBatchIndex = 0;
        this.schema = <any> schema;
        this.dictionaries = new Map();
        return this;
    }
    protected _loadRecordBatch(header: metadata.RecordBatch, body: any) {
        return new RecordBatch<T>(this.schema, header.length, this._loadVectors(header, body, this.schema.fields));
    }
    protected _loadDictionaryBatch(header: metadata.DictionaryBatch, body: any) {
        const { id, isDelta, data } = header;
        const { dictionaries, schema } = this;
        if (isDelta || !dictionaries.get(id)) {
            const type = schema.dictionaries.get(id)!;
            const vector = (isDelta ? dictionaries.get(id)!.concat(
                Vector.new(this._loadVectors(data, body, [type.dictionary])[0])) :
                Vector.new(this._loadVectors(data, body, [type.dictionary])[0])) as Vector;
            return (type.dictionaryVector = vector);
        }
        return dictionaries.get(id)!;
    }
    protected _loadVectors(header: metadata.RecordBatch, body: any, types: (Field | DataType)[]) {
        return new VectorLoader(body, header.nodes, header.buffers).visitMany(types);
    }
}

class RecordBatchStreamReaderImpl<T extends { [key: string]: DataType } = any>
    extends RecordBatchReaderImplBase<T>
        implements IRecordBatchReaderImpl<T>, IterableIterator<RecordBatch<T>> {

    constructor(protected reader: MessageReader, dictionaries = new Map<number, Vector>()) {
        super(dictionaries);
    }
    public [Symbol.iterator](): IterableIterator<RecordBatch<T>> {
        return this as IterableIterator<RecordBatch<T>>;
    }
    public close() {
        if (!this.closed && (this.closed = true)) {
            this.reset().reader.return();
            this.reader = <any> null;
            this.dictionaries = <any> null;
        }
        return this;
    }
    public open(autoClose = this.autoClose) {
        if (!this.closed) {
            this.autoClose = autoClose;
            try { this.schema || (this.schema = this.reader.readSchema()); }
            catch (e) { if (!autoClose) { throw e; } else { return this.close(); } }
        }
        return this;
    }
    public throw(value?: any): IteratorResult<any> {
        if (!this.closed && this.autoClose && (this.closed = true)) {
            return this.reset().reader.throw(value);
        }
        return ITERATOR_DONE;
    }
    public return(value?: any): IteratorResult<any> {
        if (!this.closed && this.autoClose && (this.closed = true)) {
            return this.reset().reader.return(value);
        }
        return ITERATOR_DONE;
    }
    public next(): IteratorResult<RecordBatch<T>> {
        if (this.closed) { return ITERATOR_DONE; }
        let message: Message | null, { reader } = this;
        while (message = this.readNextMessageAndValidate()) {
            if (message.isSchema()) {
                this.reset(message.header());
            } else if (message.isRecordBatch()) {
                this.recordBatchIndex++;
                const header = message.header();
                const buffer = reader.readMessageBody(message.bodyLength);
                const recordBatch = this._loadRecordBatch(header, buffer);
                return { done: false, value: recordBatch };
            } else if (message.isDictionaryBatch()) {
                this.dictionaryIndex++;
                const header = message.header();
                const buffer = reader.readMessageBody(message.bodyLength);
                const vector = this._loadDictionaryBatch(header, buffer);
                this.dictionaries.set(header.id, vector);
            }
        }
        return this.return();
    }
    protected readNextMessageAndValidate<T extends MessageHeader>(type?: T | null) {
        return this.reader.readMessage<T>(type);
    }
}

class AsyncRecordBatchStreamReaderImpl<T extends { [key: string]: DataType } = any>
    extends RecordBatchReaderImplBase<T>
        implements IRecordBatchReaderImpl<T>, AsyncIterableIterator<RecordBatch<T>> {

    constructor(protected reader: AsyncMessageReader, dictionaries = new Map<number, Vector>()) {
        super(dictionaries);
    }
    public [Symbol.asyncIterator](): AsyncIterableIterator<RecordBatch<T>> {
        return this as AsyncIterableIterator<RecordBatch<T>>;
    }
    public async close() {
        if (!this.closed && (this.closed = true)) {
            await this.reset().reader.return();
            this.reader = <any> null;
            this.dictionaries = <any> null;
        }
        return this;
    }
    public async open(autoClose = this.autoClose) {
        if (!this.closed) {
            this.autoClose = autoClose;
            try { this.schema || (this.schema = await this.reader.readSchema()); }
            catch (e) { if (!autoClose) { throw e; } else { return await this.close(); } }
        }
        return this;
    }
    public async throw(value?: any): Promise<IteratorResult<any>> {
        if (!this.closed && this.autoClose && (this.closed = true)) {
            return await this.reset().reader.throw(value);
        }
        return ITERATOR_DONE;
    }
    public async return(value?: any): Promise<IteratorResult<any>> {
        if (!this.closed && this.autoClose && (this.closed = true)) {
            return await this.reset().reader.return(value);
        }
        return ITERATOR_DONE;
    }
    public async next() {
        if (this.closed) { return ITERATOR_DONE; }
        let message: Message | null, { reader } = this;
        while (message = await this.readNextMessageAndValidate()) {
            if (message.isSchema()) {
                await this.reset(message.header());
            } else if (message.isRecordBatch()) {
                this.recordBatchIndex++;
                const header = message.header();
                const buffer = await reader.readMessageBody(message.bodyLength);
                const recordBatch = this._loadRecordBatch(header, buffer);
                return { done: false, value: recordBatch };
            } else if (message.isDictionaryBatch()) {
                this.dictionaryIndex++;
                const header = message.header();
                const buffer = await reader.readMessageBody(message.bodyLength);
                const vector = this._loadDictionaryBatch(header, buffer);
                this.dictionaries.set(header.id, vector);
            }
        }
        return await this.return();
    }
    protected async readNextMessageAndValidate<T extends MessageHeader>(type?: T | null) {
        return await this.reader.readMessage<T>(type);
    }
}

class RecordBatchFileReaderImpl<T extends { [key: string]: DataType } = any>
    extends RecordBatchStreamReaderImpl<T>
        implements IRecordBatchFileReaderImpl<T>, IterableIterator<RecordBatch<T>> {

    // @ts-ignore
    public footer: Footer;
    public get numDictionaries() { return this.footer.numDictionaries; }
    public get numRecordBatches() { return this.footer.numRecordBatches; }

    constructor(protected file: RandomAccessFile, dictionaries = new Map<number, Vector>()) {
        super(new MessageReader(file), dictionaries);
    }
    public open(autoClose = this.autoClose) {
        if (!this.closed && !this.footer) {
            this.schema = this.readFooter().schema;
        }
        return super.open(autoClose);
    }
    public readRecordBatch(index: number) {
        if (this.closed) { return null; }
        if (!this.footer) { this.open(); }
        const block = this.footer.getRecordBatch(index);
        if (block && this.file.seek(block.offset)) {
            const message = this.reader.readMessage(MessageHeader.RecordBatch);
            if (message && message.isRecordBatch()) {
                const header = message.header();
                const buffer = this.reader.readMessageBody(message.bodyLength);
                const recordBatch = this._loadRecordBatch(header, buffer);
                return recordBatch;
            }
        }
        return null;
    }
    protected readDictionaryBatch(index: number) {
        const block = this.footer.getDictionaryBatch(index);
        if (block && this.file.seek(block.offset)) {
            const message = this.reader.readMessage(MessageHeader.DictionaryBatch);
            if (message && message.isDictionaryBatch()) {
                const header = message.header();
                const buffer = this.reader.readMessageBody(message.bodyLength);
                const vector = this._loadDictionaryBatch(header, buffer);
                this.dictionaries.set(header.id, vector);
            }
        }
    }
    protected readFooter() {
        if (!this.footer) {
            const { file } = this;
            const size = file.size;
            const offset = size - magicAndPadding;
            const length = file.readInt32(offset);
            const buffer = file.readAt(offset - length, length);
            const footer = this.footer = Footer.decode(buffer);
            for (const block of footer.dictionaryBatches()) {
                block && this.readDictionaryBatch(this.dictionaryIndex++);
            }
        }
        return this.footer;
    }
    protected readNextMessageAndValidate<T extends MessageHeader>(type?: T | null): Message<T> | null {
        if (!this.footer) { this.open(); }
        if (this.recordBatchIndex < this.numRecordBatches) {
            const block = this.footer.getRecordBatch(this.recordBatchIndex);
            if (block && this.file.seek(block.offset)) {
                return this.reader.readMessage(type);
            }
        }
        return null;
    }
}

class AsyncRecordBatchFileReaderImpl<T extends { [key: string]: DataType } = any>
    extends AsyncRecordBatchStreamReaderImpl<T>
        implements IRecordBatchFileReaderImpl<T>, AsyncIterableIterator<RecordBatch<T>> {

    // @ts-ignore
    public footer: Footer;
    public get numDictionaries() { return this.footer.numDictionaries; }
    public get numRecordBatches() { return this.footer.numRecordBatches; }

    constructor(protected file: AsyncRandomAccessFile, dictionaries = new Map<number, Vector>()) {
        super(new AsyncMessageReader(file), dictionaries);
    }
    public async open(autoClose = this.autoClose) {
        if (!this.closed && !this.footer) {
            this.schema = (await this.readFooter()).schema;
        }
        return await super.open(autoClose);
    }
    public async readRecordBatch(index: number) {
        if (this.closed) { return null; }
        if (!this.footer) { await this.open(); }
        const block = this.footer.getRecordBatch(index);
        if (block && (await this.file.seek(block.offset))) {
            const message = await this.reader.readMessage(MessageHeader.RecordBatch);
            if (message && message.isRecordBatch()) {
                const header = message.header();
                const buffer = await this.reader.readMessageBody(message.bodyLength);
                const recordBatch = this._loadRecordBatch(header, buffer);
                return recordBatch;
            }
        }
        return null;
    }
    protected async readDictionaryBatch(index: number) {
        const block = this.footer.getDictionaryBatch(index);
        if (block && (await this.file.seek(block.offset))) {
            const message = await this.reader.readMessage(MessageHeader.DictionaryBatch);
            if (message && message.isDictionaryBatch()) {
                const header = message.header();
                const buffer = await this.reader.readMessageBody(message.bodyLength);
                const vector = this._loadDictionaryBatch(header, buffer);
                this.dictionaries.set(header.id, vector);
            }
        }
    }
    protected async readFooter() {
        if (!this.footer) {
            const { file } = this;
            const offset = file.size - magicAndPadding;
            const length = await file.readInt32(offset);
            const buffer = await file.readAt(offset - length, length);
            const footer = this.footer = Footer.decode(buffer);
            for (const block of footer.dictionaryBatches()) {
                block && (await this.readDictionaryBatch(this.dictionaryIndex++));
            }
        }
        return this.footer;
    }
    protected async readNextMessageAndValidate<T extends MessageHeader>(type?: T | null): Promise<Message<T> | null> {
        if (!this.footer) { await this.open(); }
        if (this.recordBatchIndex < this.numRecordBatches) {
            const block = this.footer.getRecordBatch(this.recordBatchIndex);
            if (block && await this.file.seek(block.offset)) {
                return await this.reader.readMessage(type);
            }
        }
        return null;
    }
}

class RecordBatchJSONReaderImpl<T extends { [key: string]: DataType } = any> extends RecordBatchStreamReaderImpl<T> {
    constructor(protected reader: JSONMessageReader, dictionaries = new Map<number, Vector>()) {
        super(reader, dictionaries);
    }
    protected _loadVectors(header: metadata.RecordBatch, body: any, types: (Field | DataType)[]) {
        return new JSONVectorLoader(body, header.nodes, header.buffers).visitMany(types);
    }
}

interface IRecordBatchReaderImpl<T extends { [key: string]: DataType } = any> {

    closed: boolean;
    schema: Schema<T>;
    autoClose: boolean;
    numDictionaries: number;
    numRecordBatches: number;
    dictionaries: Map<number, Vector>;

    open(autoClose?: boolean): this | Promise<this>;
    reset(schema?: Schema<T> | null): this;
    close(): this | Promise<this>;

    [Symbol.iterator]?(): IterableIterator<RecordBatch<T>>;
    [Symbol.asyncIterator]?(): AsyncIterableIterator<RecordBatch<T>>;

    throw(value?: any): IteratorResult<any> | Promise<IteratorResult<any>>;
    return(value?: any): IteratorResult<any> | Promise<IteratorResult<any>>;
    next(value?: any): IteratorResult<RecordBatch<T>> | Promise<IteratorResult<RecordBatch<T>>>;

    isSync(): this is RecordBatchFileReaderImpl<T> | RecordBatchStreamReaderImpl<T>;
    isFile(): this is RecordBatchFileReaderImpl<T> | AsyncRecordBatchFileReaderImpl<T>;
    isStream(): this is RecordBatchStreamReaderImpl<T> | AsyncRecordBatchStreamReaderImpl<T>;
    isAsync(): this is AsyncRecordBatchFileReaderImpl<T> | AsyncRecordBatchStreamReaderImpl<T>;
}

interface IRecordBatchFileReaderImpl<T extends { [key: string]: DataType } = any> extends IRecordBatchReaderImpl<T> {

    footer: Footer;

    readRecordBatch(index: number): RecordBatch<T> | null | Promise<RecordBatch<T> | null>;
}

export interface RecordBatchFileReader<T extends { [key: string]: DataType } = any> {
    close(): this;
    open(autoClose?: boolean): this;
    throw(value?: any): IteratorResult<any>;
    return(value?: any): IteratorResult<any>;
    next(value?: any): IteratorResult<RecordBatch<T>>;
    readRecordBatch(index: number): RecordBatch<T> | null;
}

export interface RecordBatchStreamReader<T extends { [key: string]: DataType } = any> {
    close(): this;
    open(autoClose?: boolean): this;
    throw(value?: any): IteratorResult<any>;
    return(value?: any): IteratorResult<any>;
    next(value?: any): IteratorResult<RecordBatch<T>>;
}

export interface AsyncRecordBatchFileReader<T extends { [key: string]: DataType } = any> {
    close(): Promise<this>;
    open(autoClose?: boolean): Promise<this>;
    throw(value?: any): Promise<IteratorResult<any>>;
    return(value?: any): Promise<IteratorResult<any>>;
    next(value?: any): Promise<IteratorResult<RecordBatch<T>>>;
    readRecordBatch(index: number): Promise<RecordBatch<T> | null>;
}

export interface AsyncRecordBatchStreamReader<T extends { [key: string]: DataType } = any> {
    close(): Promise<this>;
    open(autoClose?: boolean): Promise<this>;
    throw(value?: any): Promise<IteratorResult<any>>;
    return(value?: any): Promise<IteratorResult<any>>;
    next(value?: any): Promise<IteratorResult<RecordBatch<T>>>;
}
