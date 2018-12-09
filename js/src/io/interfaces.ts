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

// import { Readable, Writable } from 'stream';

export const ITERATOR_DONE: any = Object.freeze({ done: true, value: void (0) });

export type FileHandle = import('fs').promises.FileHandle;
// export type ReadableNodeStream = import('stream').Readable;
export type ReadableNodeStreamOptions = import('stream').ReadableOptions;

export type ReadableDOMStreamOptions = { type?: 'bytes', autoAllocateChunkSize?: number };

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

export abstract class Streamable<T> {

    public abstract asReadableDOMStream(options?: ReadableDOMStreamOptions): ReadableStream<T>;
    public abstract asReadableNodeStream(options?: ReadableNodeStreamOptions): import('stream').Readable;

    public pipe<R extends NodeJS.WritableStream>(writable: R, options?: { end?: boolean; }) {
        return this._getReadableNodeStream().pipe(writable, options);
    }
    public pipeTo(writable: WritableStream<T>, options?: PipeOptions) { return this._getReadableDOMStream().pipeTo(writable, options); }
    public pipeThrough<R extends ReadableStream<any>>(duplex: { writable: WritableStream<T>, readable: R }, options?: PipeOptions) {
        return this._getReadableDOMStream().pipeThrough(duplex, options);
    }

    private _readableDOMStream?: ReadableStream<T>;
    private _getReadableDOMStream() {
        return this._readableDOMStream || (this._readableDOMStream = this.asReadableDOMStream());
    }

    private _readableNodeStream?: import('stream').Readable;
    private _getReadableNodeStream() {
        return this._readableNodeStream || (this._readableNodeStream = this.asReadableNodeStream());
    }
}
