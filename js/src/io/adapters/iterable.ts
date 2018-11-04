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

import { joinUint8Arrays } from '../../util/buffer';
import { toUint8ArrayIterator } from '../../util/buffer';
import { toUint8ArrayAsyncIterator } from '../../util/buffer';

type TElement = ArrayBufferLike | ArrayBufferView | string;

const pump = <T extends Iterator<any> | AsyncIterator<any>>(iterator: T) => { iterator.next(); return iterator; }

/**
 * @ignore
 */
export function fromIterable<T extends TElement>(source: Iterable<T> | T): IterableIterator<Uint8Array> {
    return pump(_fromIterable<T>(source));
}

/**
 * @ignore
 */
export function fromAsyncIterable<T extends TElement>(source: AsyncIterable<T> | PromiseLike<T>): AsyncIterableIterator<Uint8Array> {
    return pump(_fromAsyncIterable<T>(source));
}

function* _fromIterable<T extends TElement>(source: Iterable<T> | T): IterableIterator<Uint8Array> {

    let cmd: 'peek' | 'read', size: number, bufferLength = 0;
    let done: boolean, it: Iterator<Uint8Array> | null = null;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | null = null;

    function byteRange() {
        if (cmd === 'peek') {
            return joinUint8Arrays(buffers.slice(), size)[0];
        }
        [buffer, buffers] = joinUint8Arrays(buffers, size);
        bufferLength -= buffer.byteLength;
        return buffer;
    }

    // Yield so the caller can inject the read command before creating the source Iterator
    ({ cmd, size } = yield <any> null);

    try {
        do {
            // initialize the iterator
            (it || (it = toUint8ArrayIterator(source)[Symbol.iterator]()));
            // read the next value
            ({ done, value: buffer } = isNaN(size - bufferLength) ?
                it.next(undefined) : it.next(size - bufferLength));
            // if chunk is not null or empty, push it onto the queue
            if (buffer && (buffer.byteLength > 0)) {
                buffers.push(buffer);
                bufferLength += buffer.byteLength;
            }
            // If we have enough bytes in our buffer, yield chunks until we don't
            if (done || size <= bufferLength) do {
                ({ cmd, size } = yield byteRange());
            } while (size < bufferLength);
        } while (!done);
        it && (typeof it.return === 'function') && (it.return());
    } catch (e) {
        it && (typeof it!.throw === 'function') && (it!.throw!(e));
    }
}

async function* _fromAsyncIterable<T extends TElement>(source: AsyncIterable<T> | PromiseLike<T>): AsyncIterableIterator<Uint8Array> {

    let cmd: 'peek' | 'read', size: number, bufferLength = 0;
    let done: boolean, it: AsyncIterator<Uint8Array> | null = null;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | null = null;

    function byteRange() {
        if (cmd === 'peek') {
            return joinUint8Arrays(buffers.slice(), size)[0];
        }
        [buffer, buffers] = joinUint8Arrays(buffers, size);
        bufferLength -= buffer.byteLength;
        return buffer;
    }

    // Yield so the caller can inject the read command before creating the source AsyncIterator
    ({ cmd, size } = yield <any> null);

    try {
        do {
            // initialize the iterator
            (it || (it = toUint8ArrayAsyncIterator(source)[Symbol.asyncIterator]()));
            // read the next value
            ({ done, value: buffer } = isNaN(size - bufferLength)
                ? await it.next(undefined)
                : await it.next(size - bufferLength));
            // if chunk is not null or empty, push it onto the queue
            if (buffer && (buffer.byteLength > 0)) {
                buffers.push(buffer);
                bufferLength += buffer.byteLength;
            }
            // If we have enough bytes in our buffer, yield chunks until we don't
            if (done || size <= bufferLength) do {
                ({ cmd, size } = yield byteRange());
            } while (size < bufferLength);
        } while (!done);
        it && (typeof it.return === 'function') && (await it.return());
    } catch (e) {
        it && (typeof it!.throw === 'function') && (await it!.throw!(e));
    }
}
