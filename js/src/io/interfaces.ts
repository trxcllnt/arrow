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

import { Readable, Writable } from 'stream';

export const ITERATOR_DONE: any = Object.freeze({ done: true, value: void (0) });

export type FileHandle = import('fs').promises.FileHandle;
export type ReadableNodeStream = import('stream').Readable;
export type WritableNodeStream = import('stream').Writable;
export type ReadableNodeStreamOptions = import('stream').ReadableOptions;

export type ReadableDOMStream<R = any> = import('whatwg-streams').ReadableStream<R>;
export type ReadableDOMStreamReader<R = any> = import('whatwg-streams').ReadableStreamDefaultReader<R>;
export type ReadableDOMStreamBYOBReader<R = any> = import('whatwg-streams').ReadableStreamBYOBReader<R>;
export type ReadableDOMStreamController<R = any> = import('whatwg-streams').ReadableStreamDefaultController<R>;
export type ReadableDOMStreamOptions = { type?: 'bytes', autoAllocateChunkSize?: number };
export type ReadableDOMStreamSource<R = any> = import('whatwg-streams').ReadableStreamSource<R>;

export type WritableDOMStream<R = any> = import('whatwg-streams').WritableStream<R>;
export type WritableDOMStreamSink<R = any> = import('whatwg-streams').WritableStreamSink<R>;
export type WritableDOMStreamController<R = any> = import('whatwg-streams').WritableStreamDefaultController<R>;

export type PipeOptions = import('whatwg-streams').PipeOptions;
export type WritableReadableDOMStreamPair<
    T extends WritableDOMStream<any>,
    U extends ReadableDOMStream<any>
> = import('whatwg-streams').WritableReadablePair<T, U>;

export const ReadableNodeStream: typeof import('stream').Readable = Readable;
export const WritableNodeStream: typeof import('stream').Writable = Writable;
export const ReadableDOMStream: typeof import('whatwg-streams').ReadableStream = (<any> global).ReadableStream;
export const WritableDOMStream: typeof import('whatwg-streams').WritableStream = (<any> global).WritableStream;

export type ArrowJSONLike = { schema: any; batches?: any[]; dictionaries?: any[]; };

/**
 * @ignore
 */
export class ArrowJSON {
    constructor(private _json: ArrowJSONLike) {}
    public get schema(): any { return this._json['schema']; }
    public get batches(): any[] { return (this._json['batches'] || []) as any[]; }
    public get dictionaries(): any[] { return (this._json['dictionaries'] || []) as any[]; }
}

export abstract class Streamable<TResult> {

    public abstract asReadableNodeStream(options?: ReadableNodeStreamOptions): ReadableNodeStream;
    public abstract asReadableDOMStream(options?: ReadableDOMStreamOptions): ReadableDOMStream<TResult>;

    public pipe<R extends NodeJS.WritableStream>(writable: R, options?: { end?: boolean; }) {
        return this._getReadableNodeStream().pipe(writable, options);
    }

    public getReader(): ReadableDOMStreamReader<TResult>;
    public getReader(readerOptions?: { mode: 'byob' }): ReadableDOMStreamBYOBReader<TResult>;
    public getReader(readerOptions?: { mode: 'byob' }) {
        const rds = this._readableDOMStream || (this._readableDOMStream = this.asReadableDOMStream());
        return readerOptions ? rds.getReader(readerOptions) : rds.getReader();
    }

    public tee() { return this._getReadableDOMStream().tee(); }
    public get locked() { return this._readableDOMStream ? this._readableDOMStream.locked : false; }
    public cancel(reason?: any) { return Promise.resolve(this._readableDOMStream && this._readableDOMStream.cancel(reason)); }
    public pipeTo(writable: WritableDOMStream<TResult>, options?: PipeOptions) { return this._getReadableDOMStream().pipeTo(writable, options); }
    public pipeThrough<R extends ReadableDOMStream<any>>(duplex: WritableReadableDOMStreamPair<WritableDOMStream<TResult>, R>, options?: PipeOptions) {
        return this._getReadableDOMStream().pipeThrough(duplex, options);
    }

    private _readableNodeStream?: ReadableNodeStream;
    private _readableDOMStream?: ReadableDOMStream<TResult>;
    private _getReadableDOMStream() {
        return this._readableDOMStream || (this._readableDOMStream = this.asReadableDOMStream());
    }
    private _getReadableNodeStream() {
        return this._readableNodeStream || (this._readableNodeStream = this.asReadableNodeStream());
    }
}
