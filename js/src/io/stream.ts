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

import { toReadableDOMStream } from './adapters/stream.dom';
import { toReadableNodeStream } from './adapters/stream.node';
import { fromReadableDOMStream } from './adapters/stream.dom';
import { fromReadableNodeStream } from './adapters/stream.node';
import { fromIterable, fromAsyncIterable } from './adapters/iterable';
import { ArrayBufferViewInput, memcpy, toUint8Array } from '../util/buffer';
import { isAsyncIterable, isReadableDOMStream, isReadableNodeStream, isPromise } from '../util/compat';
import {
    Streamable, ITERATOR_DONE, WritableDOMStreamSink,
    ReadableDOMStream, ReadableDOMStreamOptions, ReadableNodeStreamOptions,
} from './interfaces';

/**
 * @ignore
 */
export class ReadableByteStream {
    // @ts-ignore
    private source: ByteSourceIterator;
    constructor(source?: Iterable<ArrayBufferViewInput> | ArrayBufferViewInput) {
        if (source) {
            this.source = new ByteSourceIterator(fromIterable(source));
        }
    }
    public throw(value?: any) { return this.source.throw(value); }
    public return(value?: any) { return this.source.return(value); }
    public peek(size?: number | null) { return this.source.peek(size); }
    public read(size?: number | null) { return this.source.read(size); }
}

/**
 * @ignore
 */
export class AsyncReadableByteStream {
    // @ts-ignore
    private source: AsyncByteSourceIterator;
    constructor(source?: PromiseLike<ArrayBufferViewInput> | AsyncIterable<ArrayBufferViewInput> | ReadableDOMStream<ArrayBufferViewInput> | NodeJS.ReadableStream | null) {
        if (isReadableDOMStream<ArrayBufferViewInput>(source)) {
            this.source = new AsyncByteSourceIterator(fromReadableDOMStream(source));
        } else if (isReadableNodeStream(source)) {
            this.source = new AsyncByteSourceIterator(fromReadableNodeStream(source));
        } else if (isAsyncIterable<ArrayBufferViewInput>(source)) {
            this.source = new AsyncByteSourceIterator(fromAsyncIterable(source));
        } else if (isPromise<ArrayBufferViewInput>(source)) {
            this.source = new AsyncByteSourceIterator(fromAsyncIterable(source));
        }
    }
    public async throw(value?: any) { return await this.source.throw(value); }
    public async return(value?: any) { return await this.source.return(value); }
    public async peek(size?: number | null) { return await this.source.peek(size); }
    public async read(size?: number | null) { return await this.source.read(size); }
}

interface IterableByteStreamIterator extends IterableIterator<Uint8Array> {
    next(value: { cmd: 'peek' | 'read', size?: number | null }): IteratorResult<Uint8Array>;
}

interface AsyncIterableByteStreamIterator extends AsyncIterableIterator<Uint8Array> {
    next(value: { cmd: 'peek' | 'read', size?: number | null }): Promise<IteratorResult<Uint8Array>>;
}

class ByteSourceIterator {
    constructor(protected source: IterableByteStreamIterator) {}
    public throw(value?: any) { return this.source.throw && this.source.throw(value) || ITERATOR_DONE; }
    public return(value?: any) { return this.source.return && this.source.return(value) || ITERATOR_DONE; }
    public peek(size?: number | null): Uint8Array | null { return this.source.next({ cmd: 'peek', size }).value; }
    public read(size?: number | null): Uint8Array | null { return this.source.next({ cmd: 'read', size }).value; }
}

class AsyncByteSourceIterator {
    constructor(protected source: AsyncIterableByteStreamIterator) {}
    public async throw(value?: any) { return this.source.throw && await this.source.throw(value) || ITERATOR_DONE; }
    public async return(value?: any) { return this.source.return && await this.source.return(value) || ITERATOR_DONE; }
    public async peek(size?: number | null): Promise<Uint8Array | null> { return (await this.source.next({ cmd: 'peek', size })).value; }
    public async read(size?: number | null): Promise<Uint8Array | null> { return (await this.source.next({ cmd: 'read', size })).value; }
}

/**
 * @ignore
 */
export class WritableByteStream<T extends ArrayBufferViewInput> extends Streamable<Uint8Array> implements WritableDOMStreamSink<T> {

    private closed = false;
    private bytesWritten = 0;
    private chunks: (Uint8Array | number)[] = [];
    private duplex?: AsyncWritableByteStream<Uint8Array> | null;

    public align(alignment: number) {
        if (this._isOpen() && alignment > 1) {
            const a = alignment - 1;
            const p = this.bytesWritten;
            const remainder = ((p + a) & ~a) - p;
            this.chunks.push(remainder);
            this.bytesWritten += remainder;
            this.duplex && this.duplex.write(new Uint8Array(remainder));
        }
    }
    public write(value?: T | null) {
        if (this._isOpen() && value == null) {
            return this.close();
        }
        const chunk = toUint8Array(value);
        this.chunks.push(chunk);
        this.bytesWritten += chunk.byteLength;
        this.duplex && this.duplex.write(chunk);
    }
    public abort(reason: any) {
        this._isOpen();
        this.duplex && this.duplex.abort(reason);
        this.duplex = null;
        this.close();
    }
    public close() {
        if (this._isOpen() && (this.closed = true)) {
            this.chunks = [];
            this.bytesWritten = 0;
            const { duplex: sink } = this;
            this.duplex = null;
            sink && sink.close();
        }
    }
    public destroy() { this.close(); }
    public finalize(alignment = 4) {
        this.align(alignment);
        let byteOffset = 0;
        let bytes = new Uint8Array(this.bytesWritten);
        for (let chunkOrNBytes of this.chunks) {
            if (typeof chunkOrNBytes !== 'number') {
                memcpy(bytes, chunkOrNBytes, byteOffset);
                chunkOrNBytes = chunkOrNBytes.byteLength;
            }
            byteOffset += chunkOrNBytes;
        }
        this.close();
        return bytes;
    }
    public asReadableDOMStream(options?: ReadableDOMStreamOptions) {
        return this._getDuplexSink().asReadableDOMStream(options);
    }
    public asReadableNodeStream(options?: ReadableNodeStreamOptions) {
        return this._getDuplexSink().asReadableNodeStream(options);
    }
    private _getDuplexSink() {
        if (this._isOpen() && this.duplex) {
            return this.duplex;
        }
        let x: number | Uint8Array;
        const values = this.chunks;
        const duplex = new AsyncWritableByteStream<Uint8Array>();
        for (let i = -1, n = values.length; ++i < n;) {
            duplex.write(typeof (x = values[i]) !== 'number' ? x : new Uint8Array(x));
        }
        return this.duplex = duplex;
    }
    protected _isOpen() {
        if (this.closed) {
            throw new Error('WritableByteStream is closed');
        }
        return true;
    }
}

type RejectResult = { action: 'reject'; result: any; };
type ResolveResult<T> = { action: 'resolve'; result: IteratorResult<T>; };
type Resolution<T> = { resolve: (value?: T | PromiseLike<T>) => void; reject: (reason?: any) => void; }
/**
 * @ignore
 */
export class AsyncWritableByteStream<T extends ArrayBufferViewInput> extends Streamable<Uint8Array> implements WritableDOMStreamSink<T>, AsyncIterableIterator<Uint8Array> {

    private closed = false;
    private bytesWritten = 0;
    private queue: (ResolveResult<Uint8Array> | RejectResult)[] = [];
    private resolvers: Resolution<IteratorResult<Uint8Array>>[] = [];

    public align(alignment: number) {
        this._isOpen();
        const val = alignment - 1;
        const pos = this.bytesWritten;
        const remainder = ((pos + val) & ~val) - pos;
        this.write(new ArrayBuffer(remainder) as T);
    }
    public write(value?: T | null) {
        if (this._isOpen() && value == null) {
            return this.close();
        }
        const chunk = toUint8Array(value);
        const result = { done: false, value: chunk };
        this.bytesWritten += chunk.byteLength;
        this.resolvers.length > 0 ?
            this.resolvers.shift()!.resolve(result) :
            this.queue.push({ result, action: 'resolve' });
    }
    public abort(result: any) {
        this._isOpen() && this.resolvers.length > 0 ?
            this.resolvers.shift()!.reject(result) :
            this.queue.push({ result, action: 'reject' });
    }
    public close() {
        if (this._isOpen() && (this.closed = true)) {
            const { resolvers } = this;
            while (resolvers.length > 0) {
                resolvers.shift()!.resolve(ITERATOR_DONE);
            }
        }
    }
    public destroy() { this.close(); }
    public [Symbol.asyncIterator]() { return this; }
    public next(): Promise<IteratorResult<Uint8Array>> {
        if (this.queue.length > 0) {
            const { action, result } = this.queue.shift()!;
            return (Promise[action] as any)(result) as Promise<IteratorResult<Uint8Array>>;
        }
        if (this.closed) { return Promise.resolve(ITERATOR_DONE); }
        return new Promise<IteratorResult<Uint8Array>>((resolve, reject) => {
            this.resolvers.push({ resolve, reject });
        });
    }
    public asReadableDOMStream(options?: ReadableDOMStreamOptions) { return toReadableDOMStream(this, options); }
    public asReadableNodeStream(options?: ReadableNodeStreamOptions) { return toReadableNodeStream(this, options); }
    protected _isOpen() {
        if (this.closed) {
            throw new Error('AsyncWritableByteStream is closed');
        }
        return true;
    }
}
