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
import { ArrayBufferViewInput } from '../util/buffer';
import { ByteSource, AsyncByteSource } from '../io/stream';
import { ArrowJSONInput, FileHandle } from '../io/interfaces';
import { toReadableDOMStream } from '../io/adapters/stream.dom';
import { toReadableNodeStream } from '../io/adapters/stream.node';
import { ArrowJSON, ArrowStream, AsyncArrowStream, ArrowFile, AsyncArrowFile } from '../io';
import { ReadableDOMStream, WritableDOMStream, PipeOptions, WritableReadablePair } from '../io/interfaces';
import { MessageReader, AsyncMessageReader, checkForMagicArrowString, magicLength, magicX2AndPadding } from './reader/message';
import { isPromise, isArrowJSON, isFileHandle, isAsyncIterable, isReadableDOMStream, isReadableNodeStream } from '../util/compat';

abstract class AbstractRecordBatchReader<T extends { [key: string]: DataType } = any> {

    public static open<T extends { [key: string]: DataType } = any>(source: ArrowJSONInput | ArrowJSON | RecordBatchJSONReader<T>                                                 , autoClose?: boolean): RecordBatchJSONReader<T>;
    public static open<T extends { [key: string]: DataType } = any>(source: ArrowFile | RecordBatchFileReader<T>                                                                  , autoClose?: boolean): RecordBatchFileReader<T>;
    public static open<T extends { [key: string]: DataType } = any>(source: ArrowStream | RecordBatchStreamReader<T>                                                              , autoClose?: boolean): RecordBatchStreamReader<T>;
    public static open<T extends { [key: string]: DataType } = any>(source: AsyncArrowFile | AsyncRecordBatchFileReader<T>                                                        , autoClose?: boolean): Promise<AsyncRecordBatchFileReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: AsyncArrowStream | AsyncRecordBatchStreamReader<T>                                                    , autoClose?: boolean): Promise<AsyncRecordBatchStreamReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: ArrayBufferViewInput | Iterable<ArrayBufferViewInput>                                                 , autoClose?: boolean): RecordBatchFileReader<T> | RecordBatchStreamReader<T>;
    public static open<T extends { [key: string]: DataType } = any>(source: PromiseLike<ArrayBufferViewInput> | PromiseLike<Iterable<ArrayBufferViewInput>>                       , autoClose?: boolean): Promise<RecordBatchFileReader<T> | RecordBatchStreamReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: NodeJS.ReadableStream | ReadableDOMStream<ArrayBufferViewInput> | AsyncIterable<ArrayBufferViewInput> , autoClose?: boolean): Promise<RecordBatchFileReader<T> | AsyncRecordBatchStreamReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: FileHandle | PromiseLike<FileHandle>                                                                  , autoClose?: boolean): Promise<AsyncRecordBatchFileReader<T> | AsyncRecordBatchStreamReader<T>>;
    public static open<T extends { [key: string]: DataType } = any>(source: TOpenRecordBatchReaderArgs<T>                                                                         , autoClose = true) {

        if (isPromise<FileHandle>(source)) { return (async () => await AbstractRecordBatchReader.open<T>(await source, autoClose))(); }
        if (isPromise<ArrayBufferViewInput>(source)) { return (async () => await AbstractRecordBatchReader.open<T>(await source, autoClose))(); }
        if (isPromise<Iterable<ArrayBufferViewInput>>(source)) { return (async () => await AbstractRecordBatchReader.open<T>(await source, autoClose))(); }

        if (source instanceof RecordBatchJSONReader) { return source.open(autoClose) as RecordBatchJSONReader<T>; }
        if (source instanceof RecordBatchFileReader) { return source.open(autoClose) as RecordBatchFileReader<T>; }
        if (source instanceof RecordBatchStreamReader) { return source.open(autoClose) as RecordBatchStreamReader<T>; }
        if (source instanceof AsyncRecordBatchFileReader) { return source.open(autoClose) as Promise<AsyncRecordBatchFileReader<T>>; }
        if (source instanceof AsyncRecordBatchStreamReader) { return source.open(autoClose) as Promise<AsyncRecordBatchStreamReader<T>>; }

        if (source instanceof ArrowJSON) { return new RecordBatchJSONReader<T>(source).open(autoClose); }
        if (source instanceof ArrowFile) { return new RecordBatchFileReader<T>(source).open(autoClose); }
        if (source instanceof ArrowStream) { return new RecordBatchStreamReader<T>(source).open(autoClose); }
        if (source instanceof AsyncArrowFile) { return new AsyncRecordBatchFileReader<T>(source).open(autoClose); }
        if (source instanceof AsyncArrowStream) { return new AsyncRecordBatchStreamReader<T>(source).open(autoClose); }

        if (                              isArrowJSON(source)) { return openJSON<T>(source, autoClose); }
        if (                             isFileHandle(source)) { return openFileHandle<T>(source, autoClose); }
        if (isReadableDOMStream<ArrayBufferViewInput>(source)) { return openAsyncByteStream<T>(new AsyncByteSource(source), autoClose); }
        if (                     isReadableNodeStream(source)) { return openAsyncByteStream<T>(new AsyncByteSource(source), autoClose); }
        if (    isAsyncIterable<ArrayBufferViewInput>(source)) { return openAsyncByteStream<T>(new AsyncByteSource(source), autoClose); }
                                                                 return      openByteStream<T>(new      ByteSource(source), autoClose);
    }

    // @ts-ignore
    protected _schema: Schema;
    // @ts-ignore
    protected _footer: Footer;
    protected _autoClose = true;
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

    public abstract open(autoClose?: boolean): this | Promise<this>;
    public abstract close(): this | Promise<this>;

    public abstract next(): IteratorResult<RecordBatch<T>> | Promise<IteratorResult<RecordBatch<T>>>;
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

    public reset(schema: Schema | null = this._schema) {
        this._dictionaryIndex = 0;
        this._recordBatchIndex = 0;
        this._schema = <any> schema;
        this._dictionaries = new Map();
        return this;
    }

    public asReadableDOMStream() { return toReadableDOMStream<RecordBatch<T>>(<any> this); }
    public asReadableNodeStream() { return toReadableNodeStream<RecordBatch<T>>(<any> this, { objectMode: true }); }
    public pipe<R extends NodeJS.WritableStream>(writable: R, options?: { end?: boolean; }) {
        return this.asReadableNodeStream().pipe(writable, options);
    }
    public pipeTo(writable: WritableDOMStream<RecordBatch<T>>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeTo(writable, options);
    }
    public pipeThrough<R extends ReadableDOMStream<any>>(duplex: WritableReadablePair<WritableDOMStream<RecordBatch<T>>, R>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeThrough(duplex, options);
    }
}

export { AbstractRecordBatchReader as RecordBatchReader };

import { RecordBatchJSONReader } from './reader/json';
import { RecordBatchReader, AsyncRecordBatchReader } from './reader/base';
import { RecordBatchFileReader, AsyncRecordBatchFileReader } from './reader/file';
import { RecordBatchStreamReader, AsyncRecordBatchStreamReader } from './reader/stream';

function openJSON<T extends { [key: string]: DataType }>(source: ArrowJSONInput, autoClose?: boolean) {
    return new RecordBatchJSONReader<T>(new ArrowJSON(source)).open(autoClose);
}

function openByteStream<T extends { [key: string]: DataType }>(source: ByteSource, autoClose?: boolean) {
    let bytes = source.peek((magicLength + 7) & ~7);
    return bytes
        ? checkForMagicArrowString(bytes)
        ? new RecordBatchFileReader<T>(source.read()).open(autoClose)
        : new RecordBatchStreamReader<T>(source).open(autoClose)
        : new RecordBatchStreamReader<T>(function*(): any {}()).open(autoClose);
}

async function openAsyncByteStream<T extends { [key: string]: DataType }>(source: AsyncByteSource, autoClose?: boolean) {
    let bytes = await source.peek((magicLength + 7) & ~7);
    return await (bytes
        ? checkForMagicArrowString(bytes)
        ? new RecordBatchFileReader<T>(await source.read()).open(autoClose)
        : new AsyncRecordBatchStreamReader<T>(source).open(autoClose)
        : new AsyncRecordBatchStreamReader<T>(async function*(): any {}()).open(autoClose));
}

async function openFileHandle<T extends { [key: string]: DataType }>(source: FileHandle, autoClose?: boolean) {
    let { size } = await source.stat();
    let file = new AsyncArrowFile(source, size);
    if (size >= magicX2AndPadding) {
        if (checkForMagicArrowString(await file.readAt(0, (magicLength + 7) & ~7))) {
            return await new AsyncRecordBatchFileReader<T>(file).open(autoClose);
        }
    }
    return await new AsyncRecordBatchStreamReader<T>(file).open(autoClose);
}

export type TOpenRecordBatchReaderArgs<T extends { [key: string]: DataType }> =
    RecordBatchJSONReader<T>        |
    RecordBatchFileReader<T>        |
    RecordBatchStreamReader<T>      |
    AsyncRecordBatchFileReader<T>   |
    AsyncRecordBatchStreamReader<T> |
    ArrowJSON                       |
    ArrowFile                       |
    ArrowStream                     |
    AsyncArrowFile                  |
    AsyncArrowStream                |
    ArrowJSONInput                  |
    ArrayBufferViewInput                        |
    Iterable<ArrayBufferViewInput>              |
    NodeJS.ReadableStream           |
    ReadableDOMStream<ArrayBufferViewInput>     |
    AsyncIterable<ArrayBufferViewInput>         |
    FileHandle                      |
    PromiseLike<FileHandle>         |
    PromiseLike<ArrayBufferViewInput>           |
    PromiseLike<Iterable<ArrayBufferViewInput>> |
    never                           ;
