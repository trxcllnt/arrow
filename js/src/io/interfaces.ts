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
import { Readable, Writable } from 'stream';
import { fromReadableDOMStream } from './adapters/stream.dom';
import { fromReadableNodeStream } from './adapters/stream.node';
import { fromIterable, fromAsyncIterable } from './adapters/iterable';
import { toUint8Array, ArrayBufferViewInput } from '../util/buffer';
import { isIterable, isAsyncIterable, isReadableDOMStream, isReadableNodeStream } from '../util/compat';

export type ArrowJSONInput = { schema: any; batches?: any[]; dictionaries?: any[]; };

export type FileHandle = import('fs').promises.FileHandle;
export type ReadableNodeStream = import('stream').Readable;
export type WritableNodeStream = import('stream').Writable;

export type ReadableDOMStream<R = any> = import('whatwg-streams').ReadableStream<R>;
export type WritableDOMStream<R = any> = import('whatwg-streams').WritableStream<R>;

export type PipeOptions = import('whatwg-streams').PipeOptions;
export type WritableReadablePair<
    T extends WritableDOMStream<any>,
    U extends ReadableDOMStream<any>
> = import('whatwg-streams').WritableReadablePair<T, U>;

export const ReadableNodeStream: typeof import('stream').Readable = Readable;
export const WritableNodeStream: typeof import('stream').Writable = Writable;
export const ReadableDOMStream: typeof import('whatwg-streams').ReadableStream = (<any> global).ReadableStream;
export const WritableDOMStream: typeof import('whatwg-streams').WritableStream = (<any> global).WritableStream;

export const ITERATOR_DONE: any = Object.freeze({ done: true, value: void (0) });

/**
 * @ignore
 */
export class IteratorBase<TResult, TSource extends Iterator<any> = Iterator<any>> implements IterableIterator<TResult | null | void> {
    constructor(protected source: TSource) {}
    public [Symbol.iterator](): IterableIterator<TResult> { return this as IterableIterator<TResult>; }
    public next(value?: any) { return (this.source && (this.source.next(value) as any) || ITERATOR_DONE) as IteratorResult<TResult>; }
    public throw(value?: any) { return (this.source && this.source.throw && (this.source.throw(value) as any) || ITERATOR_DONE) as IteratorResult<any>; }
    public return(value?: any) { return (this.source && this.source.return && (this.source.return(value) as any) || ITERATOR_DONE) as IteratorResult<any>; }
}

/**
 * @ignore
 */
export class AsyncIteratorBase<TResult, TSource extends AsyncIterator<any> = AsyncIterator<any>> implements AsyncIterableIterator<TResult | null | void> {
    constructor(protected source: TSource) {}
    public [Symbol.asyncIterator](): AsyncIterableIterator<TResult> { return this as AsyncIterableIterator<TResult>; }
    public async next(value?: any) { return (this.source && (await this.source.next(value) as any) || ITERATOR_DONE) as IteratorResult<TResult>; }
    public async throw(value?: any) { return (this.source && this.source.throw && (await this.source.throw(value) as any) || ITERATOR_DONE) as IteratorResult<any>; }
    public async return(value?: any) { return (this.source && this.source.return && (await this.source.return(value) as any) || ITERATOR_DONE) as IteratorResult<any>; }
}

/**
 * @ignore
 */
export class ByteStream<T = Uint8Array, U = Uint8Array> extends IteratorBase<T, Iterator<U>> {
    public static from<T = Uint8Array>(source: ArrayBufferView | Iterable<ArrayBufferView>) {
        if (       source instanceof ByteStream) { return new this<T>(source);               }
        if (         ArrayBuffer.isView(source)) { return new this<T>(fromIterable(source)); }
        if (isIterable<ArrayBufferView>(source)) { return new this<T>(fromIterable(source)); }
        throw new TypeError(`Attempted to create a ByteStream from an unrecognized source`);
    }
    public write(value: U) { return super.next(value).value; }
    public tell(nBytes: number) { return super.next(new Uint8Array(nBytes)); }
    public peek(nBytes?: number) { return super.next(typeof nBytes === 'object' && nBytes || { cmd: 'peek', size: nBytes }).value; }
    public read(nBytes?: number) { return super.next(typeof nBytes === 'object' && nBytes || { cmd: 'read', size: nBytes }).value; }
}

/**
 * @ignore
 */
export class AsyncByteStream<T = Uint8Array, U = Uint8Array> extends AsyncIteratorBase<T, AsyncIterator<U>> {
    public static from<T = Uint8Array>(source: NodeJS.ReadableStream | ReadableDOMStream<ArrayBufferView> | AsyncIterable<ArrayBufferView>) {
        if (           source instanceof AsyncByteStream) { return new this<T>(source);                         }
        if (isReadableDOMStream<ArrayBufferView>(source)) { return new this<T>( fromReadableDOMStream(source)); }
        if (                isReadableNodeStream(source)) { return new this<T>(fromReadableNodeStream(source)); }
        if (    isAsyncIterable<ArrayBufferView>(source)) { return new this<T>(     fromAsyncIterable(source)); }
        throw new TypeError(`Attempted to create an AsyncByteStream from an unrecognized source`);
    }
    public async write(value: U) { return (await super.next(value)).value; }
    public async tell(nBytes: number) { return await super.next(new Uint8Array(nBytes)); }
    public async peek(nBytes?: number) { return (await super.next(typeof nBytes === 'object' && nBytes || { cmd: 'peek', size: nBytes })).value; }
    public async read(nBytes?: number) { return (await super.next(typeof nBytes === 'object' && nBytes || { cmd: 'read', size: nBytes })).value; }
}

type FileHandle = import('fs').promises.FileHandle;

/**
 * @ignore
 */
export class RandomAccessFile<T = Uint8Array, U = Uint8Array> extends ByteStream<T, U> {
    public size: number;
    public position: number;
    protected buffer: Uint8Array | null;
    constructor(buffer: Uint8Array, byteLength = buffer.byteLength) {
        super(undefined as never);
        this.position = 0;
        this.buffer = buffer;
        this.size = byteLength;
    }
    public readInt32(position: number) {
        const { buffer, byteOffset } = this.readAt(position, 4);
        return new DataView(buffer, byteOffset).getInt32(0, true);
    }
    public seek(position: number) {
        this.position = Math.min(position, this.size);
        return position < this.size;
    }
    public read(nBytes?: number | null) { return this.next(nBytes).value; }
    public readAt(position: number, nBytes: number) {
        const buf = this.buffer;
        const end = Math.min(this.size, position + nBytes);
        return buf ? buf.subarray(position, end) : new Uint8Array(nBytes);
    }
    public next(nBytes?: number | null) {
        const { buffer, size, position } = this;
        if (buffer && position < size) {
            if (typeof nBytes !== 'number') { nBytes = Infinity; }
            return {
                done: false,
                value: <any> buffer.subarray(
                    this.position,
                    this.position = Math.min(size,
                         position + Math.min(size - position, nBytes)))
            } as IteratorResult<T>;
        }
        return ITERATOR_DONE as IteratorResult<T>;
    }
    public close() { this.buffer && (this.buffer = null); }
    public throw(value?: any) { this.close(); return super.throw(value); }
    public return(value?: any) { this.close(); return super.return(value); }
}

/**
 * @ignore
 */
export class AsyncRandomAccessFile<T = Uint8Array, U = Uint8Array> extends AsyncByteStream<T, U> {
    public size: number;
    public position: number;
    protected file: FileHandle | null;
    constructor(file: FileHandle, byteLength: number) {
        super(undefined as never);
        this.file = file;
        this.position = 0;
        this.size = byteLength;
    }
    public async readInt32(position: number) {
        const { buffer, byteOffset } = await this.readAt(position, 4);
        return new DataView(buffer, byteOffset).getInt32(0, true);
    }
    public async seek(position: number) {
        this.position = Math.min(position, this.size);
        return position < this.size;
    }
    public async read(nBytes?: number | null) { return (await this.next(nBytes)).value; }
    public async readAt(position: number, nBytes: number) {
        const { file, size } = this;
        if (file && (position + nBytes) < size) {
            const end = Math.min(size, position + nBytes);
            const buffer = new Uint8Array(end - position);
            return (await file.read(buffer, 0, nBytes, position)).buffer;
        }
        return new Uint8Array(nBytes);
    }
    public async next(nBytes?: number | null) {
        const { file, size, position } = this;
        if (file && position < size) {
            if (typeof nBytes !== 'number') { nBytes = Infinity; }
            let pos = position, offset = 0, bytesRead = 0;
            let end = Math.min(size, pos + Math.min(size - pos, nBytes));
            let buffer = new Uint8Array(Math.max(0, (this.position = end) - pos));
            while ((pos += bytesRead) < end && (offset += bytesRead) < buffer.byteLength) {
                ({ bytesRead } = await file.read(buffer, offset, buffer.byteLength - offset, pos));
            }
            return { done: false, value: <any> buffer } as IteratorResult<T>;
        }
        return ITERATOR_DONE as IteratorResult<T>;
    }
    public async throw(value?: any) { await this.close(); return await super.throw(value); }
    public async return(value?: any) { await this.close(); return await super.return(value); }
    public async close() { this.file && await this.file.close().then(() => this.file = null); }
}

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
export class ArrowFile extends RandomAccessFile<ByteBuffer> {
    constructor(source: ArrayBufferViewInput) {
        super(toUint8Array(source));
    }
    public read(size?: number) {
        return new ByteBuffer(toUint8Array(super.read(size)));
    }
}

/**
 * @ignore
 */
export class AsyncArrowFile extends AsyncRandomAccessFile<ByteBuffer> {
    public async read(size?: number) {
        return new ByteBuffer(toUint8Array(await super.read(size)));
    }
}

/**
 * @ignore
 */
export class ArrowStream extends ByteStream<ByteBuffer> {
    public static from<TResult = ByteBuffer>(source: ArrayBufferView | Iterable<ArrayBufferView>) {
        return super.from.call(this, source) as ByteStream<TResult>;
    }
    public position: number = 0;
    public read(size?: number) {
        const buf = toUint8Array(super.read(size));
        this.position += buf.byteLength;
        return new ByteBuffer(buf);
    }
}

/**
 * @ignore
 */
export class AsyncArrowStream extends AsyncByteStream<ByteBuffer> {
    public static from<TResult = ByteBuffer>(source: NodeJS.ReadableStream | ReadableDOMStream<ArrayBufferView> | AsyncIterable<ArrayBufferView>) {
        return super.from.call(this, source) as AsyncByteStream<TResult>;
    }
    public position: number = 0;
    public async read(size?: number) {
        const buf = toUint8Array(await super.read(size));
        this.position += buf.byteLength;
        return new ByteBuffer(buf);
    }
}
