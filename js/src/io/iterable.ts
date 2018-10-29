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

import { concat } from '../util/buffer';
import { toUint8ArrayIterator } from '../util/buffer';
import { toUint8ArrayAsyncIterator } from '../util/buffer';

type TElement = ArrayBufferLike | ArrayBufferView | string;

export function fromIterable<T extends TElement>(source: Iterable<T> | T): IterableIterator<Uint8Array> {
    const pumped = _fromIterable<T>(source);
    pumped.next();
    return pumped;
}

export function fromAsyncIterable<T extends TElement>(source: AsyncIterable<T> | PromiseLike<T>): AsyncIterableIterator<Uint8Array> {
    const pumped = _fromAsyncIterable<T>(source);
    pumped.next();
    return pumped;
}

function* _fromIterable<T extends TElement>(source: Iterable<T> | T): IterableIterator<Uint8Array> {

    // Yield so the caller can inject bytesToRead before creating the source Iterator
    let bytesToRead = yield <any> null, bufferLength = 0;
    let done: boolean, it: Iterator<Uint8Array> | null = null;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | null = null;
    const slice = () => ((
        [buffer, buffers] = concat(buffers, bytesToRead)) &&
        (bufferLength -= buffer.byteLength) && buffer || buffer);

    try {
        do {
            // If we have enough bytes, concat the chunks and emit the buffer.
            (bytesToRead <= bufferLength) && (bytesToRead = yield slice());
            // initialize the iterator
            (it || (it = toUint8ArrayIterator(source)[Symbol.iterator]()));
            // read the next value
            ({ done, value: buffer } = it.next(bytesToRead));
            // if chunk is not null or empty, push it onto the queue
            if (buffer && (buffer.byteLength > 0)) {
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
        it && (typeof it.return === 'function') && (it.return());
    } catch (e) {
        it && (typeof it!.throw === 'function') && (it!.throw!(e));
    }
}

async function* _fromAsyncIterable<T extends TElement>(source: AsyncIterable<T> | PromiseLike<T>): AsyncIterableIterator<Uint8Array> {

    // Yield so the caller can inject bytesToRead before creating the source AsyncIterator
    let bytesToRead = yield <any> null, bufferLength = 0;
    let done: boolean, it: AsyncIterator<Uint8Array> | null = null;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | null = null;
    const slice = () => ((
        [buffer, buffers] = concat(buffers, bytesToRead)) &&
        (bufferLength -= buffer.byteLength) && buffer || buffer);

    try {
        do {
            // If we have enough bytes, concat the chunks and emit the buffer.
            (bytesToRead <= bufferLength) && (bytesToRead = yield slice());
            // initialize the iterator
            (it || (it = toUint8ArrayAsyncIterator(source)[Symbol.asyncIterator]()));
            // read the next value
            ({ done, value: buffer } = await it.next(bytesToRead));
            // if chunk is not null or empty, push it onto the queue
            if (buffer && (buffer.byteLength > 0)) {
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
        it && (typeof it.return === 'function') && (await it.return());
    } catch (e) {
        it && (typeof it!.throw === 'function') && (await it!.throw!(e));
    }
}
