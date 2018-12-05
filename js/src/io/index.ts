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

import { flatbuffers } from 'flatbuffers';
import ByteBuffer = flatbuffers.ByteBuffer;
import { ByteStream, AsyncByteStream } from './stream';
import { RandomAccessFile, AsyncRandomAccessFile } from './file';
import { toUint8Array, ArrayBufferViewInput } from '../util/buffer';
import { ArrowJSONInput, ReadableDOMStream, FileHandle } from './interfaces';

/**
 * @ignore
 */
export class ArrowJSON {
    constructor(private _json: ArrowJSONInput) {}
    public get schema(): any { return this._json['schema']; }
    public get batches(): any[] { return (this._json['batches'] || []) as any[]; }
    public get dictionaries(): any[] { return (this._json['dictionaries'] || []) as any[]; }
}

/**
 * @ignore
 */
export class ArrowStream {
    protected source: ByteStream;
    constructor(source: ByteStream | ArrayBufferViewInput | Iterable<ArrayBufferViewInput>) {
        this.source = source instanceof ByteStream ? source : new ByteStream(source);
    }
    public throw(value?: any) { return this.source.throw(value); }
    public return(value?: any) { return this.source.return(value); }
    public peek(size?: number | null) { return new ByteBuffer(toUint8Array(this.source.read(size))); }
    public read(size?: number | null) { return new ByteBuffer(toUint8Array(this.source.read(size))); }
}

/**
 * @ignore
 */
export class AsyncArrowStream {
    protected source: AsyncByteStream;
    constructor(source: AsyncByteStream | PromiseLike<ArrayBufferViewInput> | AsyncIterable<ArrayBufferViewInput> | ReadableDOMStream<ArrayBufferViewInput> | NodeJS.ReadableStream) {
        this.source = source instanceof AsyncByteStream ? source : new AsyncByteStream(source);
    }
    public async throw(value?: any) { return await this.source.throw(value); }
    public async return(value?: any) { return await this.source.return(value); }
    public async peek(size?: number) { return new ByteBuffer(toUint8Array(await this.source.peek(size))); }
    public async read(size?: number) { return new ByteBuffer(toUint8Array(await this.source.read(size))); }
}

/**
 * @ignore
 */
export class ArrowFile extends ArrowStream {
    // @ts-ignore
    protected source: RandomAccessFile;
    constructor(source: ArrayBufferViewInput) {
        super(new RandomAccessFile(toUint8Array(source)));
    }
    public get size() { return this.source.size; }
    public close() { return this.source.close(); }
    public seek(position: number) { return this.source.seek(position); }
    public readInt32(position: number) { return this.source.readInt32(position); }
    public readAt(position: number, nBytes: number) { return this.source.readAt(position, nBytes); }
    public read(nBytes?: number | null) { return new ByteBuffer(toUint8Array(this.source.read(nBytes))); }
}

/**
 * @ignore
 */
export class AsyncArrowFile extends AsyncArrowStream {
    // @ts-ignore
    protected source: AsyncRandomAccessFile;
    constructor(source: FileHandle, byteLength: number) {
        super(new AsyncRandomAccessFile(source, byteLength));
    }
    public get size() { return this.source.size; }
    public async close() { return await this.source.close(); }
    public async seek(position: number) { return await this.source.seek(position); }
    public async readInt32(position: number) { return await this.source.readInt32(position); }
    public async readAt(position: number, nBytes: number) { return await this.source.readAt(position, nBytes); }
    public async read(nBytes?: number | null) { return new ByteBuffer(toUint8Array(await this.source.read(nBytes))); }
}
