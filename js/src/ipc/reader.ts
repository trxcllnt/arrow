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
import { Message } from './metadata/message';
import { RecordBatch } from '../recordbatch';
import { iterableAsReadableDOMStream, iterableAsReadableNodeStream} from '../io/stream';
import { asyncIterableAsReadableDOMStream, asyncIterableAsReadableNodeStream } from '../io/stream';
import { MessageReader, AsyncMessageReader, checkForMagicArrowString, magicLength, magicX2AndPadding } from './message';
import { ReadableDOMStream, WritableDOMStream, ReadableNodeStream, PipeOptions, WritableReadablePair } from '../io/interfaces';
import { isPromise, isArrowJSON, isFileHandle, isIterable, isAsyncIterable, isReadableDOMStream, isReadableNodeStream } from '../util/compat';

abstract class AbstractRecordBatchReader<T extends { [key: string]: DataType } = any> {

    public static open<T extends { [key: string]: DataType } = any>(source: ArrowJSONInput | ArrowJSON | RecordBatchJSONReader<T>                                       ): RecordBatchJSONReader<T>;
    public static open<T extends { [key: string]: DataType } = any>(source: ArrowFile | RecordBatchFileReader<T>                                                        ): RecordBatchFileReader<T>;
    public static open<T extends { [key: string]: DataType } = any>(source: ArrowStream | RecordBatchStreamReader<T>                                                    ): RecordBatchStreamReader<T>;
    public static open<T extends { [key: string]: DataType } = any>(source: AsyncArrowFile | AsyncRecordBatchFileReader<T>                                              ): Promise<AsyncRecordBatchFileReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: AsyncArrowStream | AsyncRecordBatchStreamReader<T>                                          ): Promise<AsyncRecordBatchStreamReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: ArrayBufferView | Iterable<ArrayBufferView>                                                 ): RecordBatchFileReader<T> | RecordBatchStreamReader<T>;
    public static open<T extends { [key: string]: DataType } = any>(source: PromiseLike<ArrayBufferView> | PromiseLike<Iterable<ArrayBufferView>>                       ): Promise<RecordBatchFileReader<T> | RecordBatchStreamReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: NodeJS.ReadableStream | ReadableDOMStream<ArrayBufferView> | AsyncIterable<ArrayBufferView> ): Promise<RecordBatchFileReader<T> | AsyncRecordBatchStreamReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: FileHandle | PromiseLike<FileHandle>                                                        ): Promise<AsyncRecordBatchFileReader<T> | AsyncRecordBatchStreamReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: TOpenRecordBatchReaderArgs<T>) {

        if (isPromise<FileHandle>(source)) { return (async () => await AbstractRecordBatchReader.open<T>(await source))(); }
        if (isPromise<ArrayBufferView>(source)) { return (async () => await AbstractRecordBatchReader.open<T>(await source))(); }
        if (isPromise<Iterable<ArrayBufferView>>(source)) { return (async () => await AbstractRecordBatchReader.open<T>(await source))(); }

        if (source instanceof RecordBatchJSONReader) { return source.open() as RecordBatchJSONReader<T>; }
        if (source instanceof RecordBatchFileReader) { return source.open() as RecordBatchFileReader<T>; }
        if (source instanceof RecordBatchStreamReader) { return source.open() as RecordBatchStreamReader<T>; }
        if (source instanceof AsyncRecordBatchFileReader) { return source.open() as Promise<AsyncRecordBatchFileReader<T>>; }
        if (source instanceof AsyncRecordBatchStreamReader) { return source.open() as Promise<AsyncRecordBatchStreamReader<T>>; }

        if (source instanceof ArrowJSON) { return new RecordBatchJSONReader<T>(source).open(); }
        if (source instanceof ArrowFile) { return new RecordBatchFileReader<T>(source).open(); }
        if (source instanceof ArrowStream) { return new RecordBatchStreamReader<T>(source).open(); }
        if (source instanceof AsyncArrowFile) { return new AsyncRecordBatchFileReader<T>(source).open(); }
        if (source instanceof AsyncArrowStream) { return new AsyncRecordBatchStreamReader<T>(source).open(); }

        if (                         isArrowJSON(source)) { return openJSON<T>(source); }
        if (                        isFileHandle(source)) { return openFileHandle<T>(source); }
        if (isReadableDOMStream<ArrayBufferView>(source)) { return openAsyncByteStream<T>(AsyncByteStream.from(source)); }
        if (                isReadableNodeStream(source)) { return openAsyncByteStream<T>(AsyncByteStream.from(source)); }
        if (    isAsyncIterable<ArrayBufferView>(source)) { return openAsyncByteStream<T>(AsyncByteStream.from(source)); }
                                                            return      openByteStream<T>(     ByteStream.from(source));
    }

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

    protected constructor(protected source: MessageReader | AsyncMessageReader, dictionaries = new Map<number, Vector>()) {
        this._dictionaries = dictionaries;
    }

    public abstract open(): this | Promise<this>;
    public abstract throw(value?: any): any | Promise<any>;
    public abstract return(value?: any): any | Promise<any>;
    public abstract readSchema(): Schema | null | Promise<Schema | null>;
    public abstract readMessage<T extends MessageHeader>(type?: T | null): Message | null | Promise<Message | null>;

    public isSync(): this is RecordBatchReader<T> { return this instanceof RecordBatchReader; }
    public isAsync(): this is AsyncRecordBatchReader<T> { return this instanceof AsyncRecordBatchReader; }
    public isFile(): this is RecordBatchFileReader<T> | AsyncRecordBatchFileReader<T> {
        return (this instanceof RecordBatchFileReader) || (this instanceof AsyncRecordBatchFileReader);
    }
    public isStream(): this is RecordBatchStreamReader<T> | AsyncRecordBatchStreamReader<T> {
        return (this instanceof RecordBatchStreamReader) || (this instanceof AsyncRecordBatchStreamReader);
    }

    public reset(schema?: Schema | null) {
        this._schema = <any> schema;
        this._dictionaryIndex = 0;
        this._recordBatchIndex = 0;
        this._dictionaries = new Map();
        return this;
    }

    public pipe<R extends NodeJS.WritableStream>(writable: R, options?: { end?: boolean; }) {
        return this.asReadableNodeStream().pipe(writable, options);
    }
    public pipeTo(writable: WritableDOMStream<RecordBatch<T>>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeTo(writable, options);
    }
    public pipeThrough<R extends ReadableDOMStream<any>>(duplex: WritableReadablePair<WritableDOMStream<RecordBatch<T>>, R>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeThrough(duplex, options);
    }
    public asReadableDOMStream(): ReadableDOMStream<RecordBatch<T>> {
        if (isIterable<RecordBatch<T>>(this)) { return iterableAsReadableDOMStream(this); }
        if (isAsyncIterable<RecordBatch<T>>(this)) { return asyncIterableAsReadableDOMStream(this); }
        throw new Error(`'asReadableDOMStream()' can only be called on an Iterable or AsyncIterable RecordBatchReader`);
    }
    public asReadableNodeStream(): ReadableNodeStream {
        if (isIterable<RecordBatch<T>>(this)) { return iterableAsReadableNodeStream(this); }
        if (isAsyncIterable<RecordBatch<T>>(this)) { return asyncIterableAsReadableNodeStream(this); }
        throw new Error(`'asReadableNodeStream()' can only be called on an Iterable or AsyncIterable RecordBatchReader`);
    }
}

export { AbstractRecordBatchReader as RecordBatchReader };

import { RecordBatchJSONReader } from './reader/json';
import { RecordBatchReader, AsyncRecordBatchReader } from './reader/base';
import { RecordBatchFileReader, AsyncRecordBatchFileReader } from './reader/file';
import { RecordBatchStreamReader, AsyncRecordBatchStreamReader } from './reader/stream';
import {
    ByteStream,
    AsyncByteStream,
    ArrowJSON, ArrowJSONInput,
    ArrowStream, AsyncArrowStream,
    ArrowFile, AsyncArrowFile, FileHandle,
} from '../io/interfaces';

function openJSON<T extends { [key: string]: DataType }>(source: ArrowJSONInput) {
    return new RecordBatchJSONReader<T>(new ArrowJSON(source)).open();
}

function openByteStream<T extends { [key: string]: DataType }>(source: ByteStream) {
    let bytes = source.peek(magicLength);
    return bytes
        ? checkForMagicArrowString(bytes)
        ? new RecordBatchFileReader<T>(source.read()).open()
        : new RecordBatchStreamReader<T>(source).open()
        : new RecordBatchStreamReader<T>(function*(): any {}());
}

async function openAsyncByteStream<T extends { [key: string]: DataType }>(source: AsyncByteStream) {
    let bytes = await source.peek(magicLength);
    return await (bytes
        ? checkForMagicArrowString(bytes)
        ? new RecordBatchFileReader<T>(await source.read()).open()
        : new AsyncRecordBatchStreamReader<T>(source).open()
        : new AsyncRecordBatchStreamReader<T>(async function*(): any {}()).open());
}

async function openFileHandle<T extends { [key: string]: DataType }>(source: FileHandle) {
    let { size } = await source.stat();
    let file = new AsyncArrowFile(source, size);
    if (size >= magicX2AndPadding) {
        if (checkForMagicArrowString(await file.readAt(0, magicLength))) {
            return await new AsyncRecordBatchFileReader<T>(file).open();
        }
    }
    return await new AsyncRecordBatchStreamReader<T>(file).open();
}

export type TOpenRecordBatchReaderArgs<T extends { [key: string]: DataType }> =
    RecordBatchJSONReader<T>               |
    RecordBatchFileReader<T>               |
    RecordBatchStreamReader<T>             |
    AsyncRecordBatchFileReader<T>          |
    AsyncRecordBatchStreamReader<T>        |
    ArrowJSON                              |
    ArrowFile                              |
    ArrowStream                            |
    AsyncArrowFile                         |
    AsyncArrowStream                       |
    ArrowJSONInput                         |
    ArrayBufferView                        |
    Iterable<ArrayBufferView>              |
    NodeJS.ReadableStream                  |
    ReadableDOMStream<ArrayBufferView>     |
    AsyncIterable<ArrayBufferView>         |
    FileHandle                             |
    PromiseLike<FileHandle>                |
    PromiseLike<ArrayBufferView>           |
    PromiseLike<Iterable<ArrayBufferView>> |
    never                                  ;
