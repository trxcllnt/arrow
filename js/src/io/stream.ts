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

import { ReadableDOMStream, ReadableNodeStream } from './interfaces';

export function iterableAsReadableDOMStream<T>(self: Iterable<T>) {
    let it: Iterator<T>;
    return new ReadableDOMStream<T>({
        cancel: close.bind(null, 'return'),
        start() { it = self[Symbol.iterator](); },
        pull(controller) {
            try {
                let size = controller.desiredSize;
                let r: IteratorResult<T> | null = null;
                while ((size == null || size-- > 0) && !(r = it.next()).done) {
                    controller.enqueue(r.value);
                }
                r && r.done && [close('return'), controller.close()];
            } catch (e) {
                close('throw', e);
                controller.error(e);
            }
        }
    });
    function close(signal: 'throw' | 'return', value?: any) {
        if (it && typeof it[signal] === 'function') {
            it[signal]!(value);
        }
    }
}

export function iterableAsReadableNodeStream<T>(self: Iterable<T>) {
    let it: Iterator<T>;
    return new ReadableNodeStream({
        objectMode: true,
        read(size: number) {
            (it || (it = self[Symbol.iterator]())) && read(this, size);
        },
        destroy(e: Error | null, cb: (e: Error | null) => void) {
            const signal = e == null ? 'return' : 'throw';
            try { close(signal, e); } catch (err) {
                return cb && Promise.resolve(err).then(cb);
            }
            return cb && Promise.resolve(null).then(cb);
        }
    });
    function read(sink: ReadableNodeStream, size: number) {
        let r: IteratorResult<T> | null = null;
        while ((size == null || size-- > 0) && !(r = it.next()).done) {
            if (!sink.push(r.value)) { return; }
        }
        r && r.done && end(sink);
    }
    function close(signal: 'throw' | 'return', value?: any) {
        if (it && typeof it[signal] === 'function') {
            it[signal]!(value);
        }
    }
    function end(sink: ReadableNodeStream) {
        sink.push(null);
        close('return');
    }
}

export function asyncIterableAsReadableDOMStream<T>(source: AsyncIterable<T>) {
    let it: AsyncIterator<T>;
    return new ReadableDOMStream<T>({
        cancel: close.bind(null, 'return'),
        async start() { it = source[Symbol.asyncIterator](); },
        async pull(controller) {
            try {
                let size = controller.desiredSize;
                let r: IteratorResult<T> | null = null;
                while ((size == null || size > 0) && !(r = await it.next()).done) {
                    controller.enqueue(r.value);
                }
                r && r.done && (await Promise.all([close('return'), controller.close()]));
            } catch (e) {
                await Promise.all([close('throw', e), controller.error(e)]);
            }
        }
    });
    async function close(signal: 'throw' | 'return', value?: any) {
        if (it && typeof it[signal] === 'function') {
            await it[signal]!(value);
        }
    }
}

export function asyncIterableAsReadableNodeStream<T>(source: AsyncIterable<T>) {
    let it: AsyncIterator<T>;
    return new ReadableNodeStream({
        objectMode: true,
        read(size: number) {
            (it || (it = source[Symbol.asyncIterator]())) && read(this, size);
        },
        destroy(e: Error | null, cb: (e: Error | null) => void) {
            close(e == null ? 'return' : 'throw', e).then(cb as any, cb);
        }
    });
    async function read(sink: ReadableNodeStream, size: number) {
        let r: IteratorResult<T> | null = null;
        while ((size == null || size-- > 0) && !(r = await it.next()).done) {
            if (!sink.push(r.value)) { return; }
        }
        r && r.done && await end(sink);
    }
    async function close(signal: 'throw' | 'return', value?: any) {
        if (it && typeof it[signal] === 'function') {
            await it[signal]!(value);
        }
    }
    async function end(sink: ReadableNodeStream) {
        sink.push(null);
        await close('return');
    }
}
