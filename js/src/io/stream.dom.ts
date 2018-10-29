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

import { concat, toUint8Array } from '../util/buffer';
const ReadableDOMStream: typeof import('whatwg-streams').ReadableStream = <any> ReadableStream;

type TElement = ArrayBufferLike | ArrayBufferView | string;
type ReadableStream<R = any> = import('whatwg-streams').ReadableStream<R>;
type ReadableStreamDefaultReader<R = any> = import('whatwg-streams').ReadableStreamDefaultReader<R>;

export function fromReadableDOMStream<T extends TElement>(source: ReadableStream<T>): AsyncIterableIterator<Uint8Array> {
    const pumped = _fromReadableDOMStream<T>(source);
    pumped.next();
    return pumped;
}

export function toReadableDOMStream(source: Iterable<Uint8Array> | AsyncIterable<Uint8Array>) {

    let done = false, value: Uint8Array, it: Iterator<Uint8Array> | AsyncIterator<Uint8Array>;

    const close = async (method: 'throw' | 'return', value?: any) =>
        (it && (typeof it[method] === 'function') &&
            ({ done } = await it[method]!(value)) ||
        (done = true)) && (it = null as any);

    const createIterator = () => {
        const xs: any = source;
        const fn = xs[Symbol.asyncIterator] || xs[Symbol.iterator];
        return fn.call(source);
    };

    return new ReadableDOMStream<Uint8Array>({
        cancel: close.bind(null, 'return'),
        async start() { it = createIterator(); },
        async pull(c) {
            try {
                while (!done) {
                    ({ done, value } = await it.next(c.desiredSize));
                    if (value && value.length > 0) {
                        return c.enqueue(value);
                    }
                }
                return (await close('return')) || c.close();
            } catch (e) { (await close('throw', e)) || c.error(e); }
        }
    });
}

async function* _fromReadableDOMStream<T extends TElement>(source: ReadableStream<T>): AsyncIterableIterator<Uint8Array> {

    let done = false, value: T;
    // Yield so the caller can inject bytesToRead before we establish the ReadableStream lock
    let bytesToRead = yield <any> null, bufferLength = 0;
    let it: ReadableStreamDefaultReader<T> | null = null;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | null = null;
    const slice = () => ((
        [buffer, buffers] = concat(buffers, bytesToRead)) &&
        (bufferLength -= buffer.byteLength) && buffer || buffer);

    try {
        do {
            // If we have enough bytes, concat the chunks and emit the buffer.
            (bytesToRead <= bufferLength) && (bytesToRead = yield slice());

            // initialize the reader and lock the stream
            (it || (it = source['getReader']()));
            // read the next value
            ({ done, value } = await it['read']());
            // if chunk is not null or empty, push it onto the queue
            if ((buffer = toUint8Array(value)) && (buffer.byteLength > 0)) {
                buffers.push(buffer);
                bufferLength += buffer.byteLength;
            }
            // if we're done, emit the rest of the chunks
            if (done) {
                do {
                    bytesToRead = yield slice();
                } while (bytesToRead < bufferLength);
            }
        } while (!done);
    } catch (e) {
        try { it && source['locked'] && it!['releaseLock'](); } catch (e) {}
    }
}
