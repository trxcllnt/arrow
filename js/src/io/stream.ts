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

import { ArrayBufferViewInput } from '../util/buffer';
import { fromReadableDOMStream } from './adapters/stream.dom';
import { fromReadableNodeStream } from './adapters/stream.node';
import { fromIterable, fromAsyncIterable } from './adapters/iterable';
import { ITERATOR_DONE, ReadableDOMStream } from './interfaces';
import { isAsyncIterable, isReadableDOMStream, isReadableNodeStream, isPromise } from '../util/compat';

/**
 * @ignore
 */
export class ByteSource {
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
export class AsyncByteSource {
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
