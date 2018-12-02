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
