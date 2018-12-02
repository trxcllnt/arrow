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

export abstract class AbstractRecordBatchReader<TSource extends MessageReader | AsyncMessageReader> {

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

    public abstract next(value?: any): IteratorResult<RecordBatch> | Promise<IteratorResult<RecordBatch>>;
    public    throw(value?: any) { return this.source.throw(value) as ReturnType<TSource['throw']>;      }
    public   return(value?: any) { return this.source.return(value) as ReturnType<TSource['return']>;    }
    public   isSync(): this is RecordBatchReader       { return this instanceof RecordBatchReader;       }
    public   isFile(): this is RecordBatchFileReader   { return this instanceof RecordBatchFileReader;   }
    public  isAsync(): this is AsyncRecordBatchReader  { return this instanceof AsyncRecordBatchReader;  }
    public isStream(): this is RecordBatchStreamReader { return this instanceof RecordBatchStreamReader; }

    public abstract open(): this | Promise<this>;
    public abstract readSchema(): Schema | null | Promise<Schema | null>;
    public abstract readMessage<T extends MessageHeader>(type?: T | null): Message | null | Promise<Message | null>;

    public abstract asReadableDOMStream(): ReadableDOMStream<RecordBatch>;
    public abstract asReadableNodeStream(): ReadableNodeStream;

    protected _reset(schema?: Schema | null) {
        this._schema = <any> schema;
        this._dictionaryIndex = 0;
        this._recordBatchIndex = 0;
        this._dictionaries = new Map();
        return this;
    }
    public pipe<T extends NodeJS.WritableStream>(writable: T, options?: { end?: boolean; }) {
        return this.asReadableNodeStream().pipe(writable, options);
    }
    public pipeTo(writable: WritableDOMStream<RecordBatch>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeTo(writable, options);
    }
    public pipeThrough<T extends ReadableDOMStream<any>>(duplex: WritableReadablePair<WritableDOMStream<RecordBatch>, T>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeThrough(duplex, options);
    }
}

import { Vector } from '../../vector';
import { Schema } from '../../schema';
import { MessageHeader } from '../../enum';
import { Footer } from '../metadata/file';
import { Message } from '../metadata/message';
import { RecordBatch } from '../../recordbatch';
import { RecordBatchReader } from './recordbatchreader';
import { MessageReader } from '../message/messagereader';
import { AsyncMessageReader } from '../message/asyncmessagereader';
import { AsyncRecordBatchReader } from './asyncrecordbatchreader';
import { RecordBatchFileReader, RecordBatchStreamReader } from '../reader';
import { ReadableDOMStream, WritableDOMStream, ReadableNodeStream, PipeOptions, WritableReadablePair } from '../../io/interfaces';
