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

import { Readable } from 'stream';
import { fromReadableDOMStream } from '../io/stream.dom';
import { fromReadableNodeStream } from '../io/stream.node';
import { fromIterable, fromAsyncIterable } from '../io/iterable';
import { isPromise, isAsyncIterable, isReadableNodeStream, isReadableDOMStream } from '../util/compat';

type TElement = ArrayBufferLike | ArrayBufferView | string;
type ReadableStream<R = any> = import('whatwg-streams').ReadableStream<R>;

export const ITERATOR_DONE: any = Object.freeze({ done: true, value: void (0) });

export class IteratorBase<TResult, TSource extends Iterator<any> = Iterator<any>> implements Required<IterableIterator<TResult | null | void>> {
    constructor(protected source: TSource) {}
    [Symbol.iterator]() { return this; }

    isAsync(): this is AsyncIterator<TResult> { return false; }

    next(value?: any) { return this.source && (this.source.next(value) as any) || ITERATOR_DONE; }
    throw(value?: any) { return this.source && this.source.throw && (this.source.throw(value) as any) || ITERATOR_DONE; }
    return(value?: any) { return this.source && this.source.return && (this.source.return(value) as any) || ITERATOR_DONE; }
}

export class AsyncIteratorBase<TResult, TSource extends AsyncIterator<any> = AsyncIterator<any>> implements Required<AsyncIterableIterator<TResult | null | void>> {
    constructor(protected source: TSource) {}
    [Symbol.asyncIterator]() { return this; }

    isAsync(): this is AsyncIterator<TResult> { return true; }

    async next(value?: any) { return this.source && (await this.source.next(value) as any) || ITERATOR_DONE; }
    async throw(value?: any) { return this.source && this.source.throw && (await this.source.throw(value) as any) || ITERATOR_DONE; }
    async return(value?: any) { return this.source && this.source.return && (await this.source.return(value) as any) || ITERATOR_DONE; }
}

export class ByteStream extends IteratorBase<ByteBuffer, Iterator<Uint8Array>> {

    static new<T extends TElement>(source: T | Iterable<T>): ByteStream;
    static new<T extends TElement>(source: Readable | ReadableStream<T> | PromiseLike<T> | AsyncIterable<T>): AsyncByteStream;
    static new<T extends TElement>(source: any) {
        return (
            (isReadableDOMStream<T>(source)) ? (new AsyncByteStream(fromReadableDOMStream<T>(source))) :
            (  isReadableNodeStream(source)) ? (new AsyncByteStream(  fromReadableNodeStream(source))) :
            (    isAsyncIterable<T>(source)) ? (new AsyncByteStream(    fromAsyncIterable<T>(source))) :
            (          isPromise<T>(source)) ? (new AsyncByteStream(    fromAsyncIterable<T>(source))) :
                                               (new      ByteStream(    fromIterable<T>(source as T))));
    }

    next(size?: number): IteratorResult<ByteBuffer> {
        const r = this.source.next(size) as IteratorResult<any>;
        !r.done && (r.value = new ByteBuffer(r.value));
        return r as IteratorResult<ByteBuffer>;
    }
}

export class AsyncByteStream extends AsyncIteratorBase<ByteBuffer, AsyncIterator<Uint8Array>> {
    async next(size?: number): Promise<IteratorResult<ByteBuffer>> {
        const r = (await this.source.next(size)) as IteratorResult<any>;
        !r.done && (r.value = new ByteBuffer(r.value));
        return r as IteratorResult<ByteBuffer>;
    }
}
