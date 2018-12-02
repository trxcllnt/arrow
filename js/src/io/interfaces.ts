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
import { OptionallyAsync } from '../interfaces';

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
export class IteratorBase<TResult, TSource extends Iterator<any> = Iterator<any>> implements Required<IterableIterator<TResult | null | void>> {
    constructor(protected source: TSource) {}
    [Symbol.iterator](): IterableIterator<TResult> { return this as IterableIterator<TResult>; }
    next(value?: any) { return (this.source && (this.source.next(value) as any) || ITERATOR_DONE) as IteratorResult<TResult>; }
    throw(value?: any) { return (this.source && this.source.throw && (this.source.throw(value) as any) || ITERATOR_DONE) as IteratorResult<any>; }
    return(value?: any) { return (this.source && this.source.return && (this.source.return(value) as any) || ITERATOR_DONE) as IteratorResult<any>; }
}

/**
 * @ignore
 */
export class AsyncIteratorBase<TResult, TSource extends AsyncIterator<any> = AsyncIterator<any>> implements Required<AsyncIterableIterator<TResult | null | void>> {
    constructor(protected source: TSource) {}
    [Symbol.asyncIterator](): AsyncIterableIterator<TResult> { return this as AsyncIterableIterator<TResult>; }
    async next(value?: any) { return (this.source && (await this.source.next(value) as any) || ITERATOR_DONE) as IteratorResult<TResult>; }
    async throw(value?: any) { return (this.source && this.source.throw && (await this.source.throw(value) as any) || ITERATOR_DONE) as IteratorResult<any>; }
    async return(value?: any) { return (this.source && this.source.return && (await this.source.return(value) as any) || ITERATOR_DONE) as IteratorResult<any>; }
}

/**
 * @ignore
 */
export class ByteStream<TResult = Uint8Array, TSource = Uint8Array> extends IteratorBase<TResult, Iterator<TSource>> {
    write(value: TSource) { return super.next(value).value; }
    tell(nBytes: number) { return super.next(new Uint8Array(nBytes)); }
    peek(nBytes?: number) { return super.next(typeof nBytes === 'object' && nBytes || { cmd: 'peek', size: nBytes }).value; }
    read(nBytes?: number) { return super.next(typeof nBytes === 'object' && nBytes || { cmd: 'read', size: nBytes }).value; }
}

/**
 * @ignore
 */
export class AsyncByteStream<TResult = Uint8Array, TSource = Uint8Array> extends AsyncIteratorBase<TResult, AsyncIterator<TSource>> {
    async write(value: TSource) { return (await super.next(value)).value; }
    async tell(nBytes: number) { return await super.next(new Uint8Array(nBytes)); }
    async peek(nBytes?: number) { return (await super.next(typeof nBytes === 'object' && nBytes || { cmd: 'peek', size: nBytes })).value; }
    async read(nBytes?: number) { return (await super.next(typeof nBytes === 'object' && nBytes || { cmd: 'read', size: nBytes })).value; }
}

type FileHandle = import('fs').promises.FileHandle;

/**
 * @ignore
 */
export class RandomAccessFile<T = Uint8Array, U = Uint8Array> extends ByteStream<T, U> implements OptionallyAsync<RandomAccessFile<T, U>> {
    isSync(): this is RandomAccessFile<T, U> { return true; }
    isAsync(): this is AsyncRandomAccessFile<T, U> { return false; }
    public size: number;
    public position: number;
    protected buffer: Uint8Array | null;
    constructor(buffer: Uint8Array, byteLength = buffer.byteLength) {
        super(undefined as never);
        this.position = 0;
        this.buffer = buffer;
        this.size = byteLength;
    }
    readInt32(position: number) {
        const { buffer, byteOffset } = this.readAt(position, 4);
        return new Int32Array(buffer, byteOffset, 1)[0];
    }
    seek(position: number) {
        this.position = Math.min(position, this.size);
        return position < this.size;
    }
    read(nBytes?: number | null) { return this.next(nBytes).value; }
    readAt(position: number, nBytes: number) {
        const buf = this.buffer;
        const end = Math.min(this.size, position + nBytes);
        return buf ? buf.subarray(position, end) : new Uint8Array(nBytes);
    }
    next(nBytes?: number | null) {
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
    close() { this.buffer && (this.buffer = null); }
    throw(value?: any) { this.close(); return super.throw(value); }
    return(value?: any) { this.close(); return super.return(value); }
}

/**
 * @ignore
 */
export class AsyncRandomAccessFile<T = Uint8Array, U = Uint8Array> extends AsyncByteStream<T, U> implements OptionallyAsync<RandomAccessFile<T, U>> {
    isSync(): this is RandomAccessFile<T, U> { return false; }
    isAsync(): this is AsyncRandomAccessFile<T, U> { return true; }
    public size: number;
    public position: number;
    protected file: FileHandle | null;
    constructor(file: FileHandle, byteLength: number) {
        super(undefined as never);
        this.file = file;
        this.position = 0;
        this.size = byteLength;
    }
    async readInt32(position: number) {
        const { buffer, byteOffset } = await this.readAt(position, 4);
        return new Int32Array(buffer, byteOffset, 1)[0];
    }
    async seek(position: number) {
        this.position = Math.min(position, this.size);
        return position < this.size;
    }
    async read(nBytes?: number | null) { return (await this.next(nBytes)).value; }
    async readAt(position: number, nBytes: number) {
        const { file, size } = this;
        if (file && (position + nBytes) < size) {
            const end = Math.min(size, position + nBytes);
            const buffer = new Uint8Array(end - position);
            return (await file.read(buffer, 0, nBytes, position)).buffer;
        }
        return new Uint8Array(nBytes);
    }
    async next(nBytes?: number | null) {
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
    async throw(value?: any) { await this.close(); return await super.throw(value); }
    async return(value?: any) { await this.close(); return await super.return(value); }
    async close() { this.file && await this.file.close().then(() => this.file = null); }
}
