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

import streamAdapters from './adapters';
import { ArrayBufferViewInput, toUint8Array } from '../util/buffer';
import { isAsyncIterable, isReadableDOMStream, isReadableNodeStream, isPromise } from '../util/compat';
import { Streamable, ITERATOR_DONE, ReadableDOMStreamOptions } from './interfaces';

/**
 * @ignore
 */
export class ReadableByteStream {
    // @ts-ignore
    private source: ByteSourceIterator;
    constructor(source?: Iterable<ArrayBufferViewInput> | ArrayBufferViewInput) {
        if (source) {
            this.source = new ByteSourceIterator(streamAdapters.fromIterable(source));
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
    constructor(source?: PromiseLike<ArrayBufferViewInput> | AsyncIterable<ArrayBufferViewInput> | ReadableStream<ArrayBufferViewInput> | NodeJS.ReadableStream | null) {
        if (isReadableDOMStream<ArrayBufferViewInput>(source)) {
            this.source = new AsyncByteSourceIterator(streamAdapters.fromReadableDOMStream(source));
        } else if (isReadableNodeStream(source)) {
            this.source = new AsyncByteSourceIterator(streamAdapters.fromReadableNodeStream(source));
        } else if (isAsyncIterable<ArrayBufferViewInput>(source)) {
            this.source = new AsyncByteSourceIterator(streamAdapters.fromAsyncIterable(source));
        } else if (isPromise<ArrayBufferViewInput>(source)) {
            this.source = new AsyncByteSourceIterator(streamAdapters.fromAsyncIterable(source));
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

type RejectResult = { action: 'reject'; result: any; };
type ResolveResult<T> = { action: 'resolve'; result: IteratorResult<T>; };
type Resolution<T> = { resolve: (value?: T | PromiseLike<T>) => void; reject: (reason?: any) => void; };
/**
 * @ignore
 */
export class AsyncWritableByteStream<T extends ArrayBufferViewInput> extends Streamable<Uint8Array>
    implements UnderlyingSink<T>, Partial<NodeJS.WritableStream>, AsyncIterableIterator<Uint8Array> {

    private closed = false;
    private bytesWritten = 0;
    private queue: (ResolveResult<Uint8Array> | RejectResult)[] = [];
    private resolvers: Resolution<IteratorResult<Uint8Array>>[] = [];

    public align(alignment: number, ...args: any[]) {
        this._isOpen();
        const a = alignment - 1;
        const pos = this.bytesWritten;
        const remainder = ((pos + a) & ~a) - pos;
        if (remainder > 0) {
            return this.write(new ArrayBuffer(remainder), ...args);
        }
        args.forEach((x) => typeof x === 'function' && x());
        return this.resolvers.length < 0;
    }
    public write(value?: any, ...args: any[]): any {
        if (this._isOpen() && value == null) {
            return Boolean(this.close(...args));
        }
        const chunk = toUint8Array(value);
        const result = { done: false, value: chunk };
        this.bytesWritten += chunk.byteLength;
        this.resolvers.length > 0 ?
            this.resolvers.shift()!.resolve(result) :
            this.queue.push({ result, action: 'resolve' });
        args.forEach((x) => typeof x === 'function' && x());
        return this.resolvers.length < 0;
    }
    public abort(result: any) {
        this._isOpen() && this.resolvers.length > 0 ?
            this.resolvers.shift()!.reject(result) :
            this.queue.push({ result, action: 'reject' });
    }
    public close(...args: any[]) {
        if (!this.closed && (this.closed = true)) {
            const { resolvers } = this;
            while (resolvers.length > 0) {
                resolvers.shift()!.resolve(ITERATOR_DONE);
            }
        }
        args.forEach((x) => typeof x === 'function' && x());
    }

    public [Symbol.asyncIterator]() { return this; }
    public asReadableDOMStream(options?: ReadableDOMStreamOptions) {
        return streamAdapters.toReadableDOMStream(this, options);
    }
    public asReadableNodeStream(options?: import('stream').ReadableOptions) {
        return streamAdapters.toReadableNodeStream(this, options);
    }

    public end(...args: any[]) { this.close(...args); }
    public final(...args: any[]) { this.close(...args); }
    public destroy(...args: any[]) { this.close(...args); }
    public throw(_?: any) { this.abort(_); return ITERATOR_DONE; };
    public return(_?: any) { this.close(); return ITERATOR_DONE; };
    public next(_?: any): Promise<IteratorResult<Uint8Array>> {
        if (this.queue.length > 0) {
            const { action, result } = this.queue.shift()!;
            return (Promise[action] as any)(result) as Promise<IteratorResult<Uint8Array>>;
        }
        if (this.closed) { return Promise.resolve(ITERATOR_DONE); }
        return new Promise<IteratorResult<Uint8Array>>((resolve, reject) => {
            this.resolvers.push({ resolve, reject });
        });
    }

    protected _isOpen() {
        if (this.closed) {
            throw new Error('AsyncWritableByteStream is closed');
        }
        return true;
    }
}
