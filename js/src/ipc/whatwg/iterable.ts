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

import { ReadableDOMStreamOptions } from '../../io/interfaces';
import { isIterable, isAsyncIterable } from '../../util/compat';

export function toReadableDOMStream<T>(source: Iterable<T> | AsyncIterable<T>, options?: ReadableDOMStreamOptions): ReadableStream<T> {
    if (isAsyncIterable<T>(source)) { return asyncIterableAsReadableDOMStream(source, options); }
    if (isIterable<T>(source)) { return iterableAsReadableDOMStream(source, options); }
    throw new Error(`toReadableDOMStream() must be called with an Iterable or AsyncIterable`);
}

function iterableAsReadableDOMStream<T>(source: Iterable<T>, options?: ReadableDOMStreamOptions) {

    let it: Iterator<T> | null = null;
    const bm = (options && options.type === 'bytes');

    return new ReadableStream<T>({
        ...options as any,
        start(controller) { next(controller, it || (it = source[Symbol.iterator]())); },
        pull(controller) { it ? (next(controller, it)) : controller.close(); },
        cancel() { (it && (it.return && it.return()) || true) && (it = null); }
    });

    function next(controller: ReadableStreamDefaultController<T>, it: Iterator<T>) {
        let size = controller.desiredSize;
        let r: IteratorResult<T> | null = null;
        while (!(r = it.next(bm ? size : null)).done) {
            controller.enqueue(r.value);
            if (size != null) {
                size -= (bm && ArrayBuffer.isView(r.value) ? r.value.byteLength : 1);
                if (size <= 0) { return; }
            }
        }
        controller.close();
    }
}

function asyncIterableAsReadableDOMStream<T>(source: AsyncIterable<T>, options?: ReadableDOMStreamOptions) {

    let it: AsyncIterator<T> | null = null;
    const bm = (options && options.type === 'bytes');

    return new ReadableStream<T>({
        ...options as any,
        async start(controller) { await next(controller, it || (it = source[Symbol.asyncIterator]())); },
        async pull(controller) { it ? (await next(controller, it)) : controller.close(); },
        async cancel() { (it && (it.return && await it.return()) || true) && (it = null); },
    });

    async function next(controller: ReadableStreamDefaultController<T>, it: AsyncIterator<T>) {
        let size = controller.desiredSize;
        let r: IteratorResult<T> | null = null;
        while (!(r = await it.next(bm ? size : null)).done) {
            controller.enqueue(r.value);
            if (size != null) {
                size -= (bm && ArrayBuffer.isView(r.value) ? r.value.byteLength : 1);
                if (size <= 0) { return; }
            }
        }
        controller.close();
    }
}
