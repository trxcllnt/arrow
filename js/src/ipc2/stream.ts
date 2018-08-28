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

import { Readable as NodeStream } from 'stream';
import { concat, toUint8Array } from '../util/buffer';
import { toUint8ArrayIterator as toUint8s } from '../util/buffer';
import { toUint8ArrayAsyncIterator as toUint8sAsync } from '../util/buffer';
import { ReadableStream, ReadableStreamDefaultReader } from 'whatwg-streams';

type TElement = ArrayBufferLike | ArrayBufferView | string;

export function* fromIterable<T extends TElement>(source: Iterable<T> | T): IterableIterator<Uint8Array> {

    let bytesToRead = -1, bufferLength = 0;
    let done: boolean, it: Iterator<Uint8Array> | null = null;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | null = null;
    const slice = () => ((
        [buffer, buffers] = concat(buffers, bytesToRead)) &&
        (bufferLength -= buffer.byteLength) && buffer || buffer);

    try {
        do {
            // If we have enough bytes, concat the chunks and emit the buffer.
            // On the first iteration, this allows the caller to inject bytesToRead
            // before creating the source Iterator.
            (bytesToRead && (bytesToRead <= bufferLength)) &&
                            (bytesToRead = yield slice());
            // initialize the iterator
            (it || (it = toUint8s(source)[Symbol.iterator]()));
            // read the next value
            ({ done, value: buffer } = it.next(bytesToRead));
            // if chunk is not null or empty, push it onto the queue
            if (buffer && (buffer.byteLength > 0)) {
                buffers.push(buffer);
                bufferLength += buffer.byteLength;
            }
            // if we're done, emit the rest of the chunks
            if (done) while ((bytesToRead = yield slice()) < bufferLength);
        } while (!done);
        it && (typeof it.return === 'function') && (it.return());
    } catch (e) {
        it && (typeof it!.throw === 'function') && (it!.throw!(e));
    }
}

export async function* fromAsyncIterable<T extends TElement>(source: AsyncIterable<T> | PromiseLike<T>): AsyncIterableIterator<Uint8Array> {

    let bytesToRead = -1, bufferLength = 0;
    let done: boolean, it: AsyncIterator<Uint8Array> | null = null;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | null = null;
    const slice = () => ((
        [buffer, buffers] = concat(buffers, bytesToRead)) &&
        (bufferLength -= buffer.byteLength) && buffer || buffer);

    try {
        do {
            // If we have enough bytes, concat the chunks and emit the buffer.
            // On the first iteration, this allows the caller to inject bytesToRead
            // before creating the source Iterator.
            (bytesToRead && (bytesToRead <= bufferLength)) &&
                            (bytesToRead = yield slice());
            // initialize the iterator
            (it || (it = toUint8sAsync(source)[Symbol.asyncIterator]()));
            // read the next value
            ({ done, value: buffer } = await it.next(bytesToRead));
            // if chunk is not null or empty, push it onto the queue
            if (buffer && (buffer.byteLength > 0)) {
                buffers.push(buffer);
                bufferLength += buffer.byteLength;
            }
            // if we're done, emit the rest of the chunks
            if (done) while ((bytesToRead = yield slice()) < bufferLength);
        } while (!done);
        it && (typeof it.return === 'function') && (await it.return());
    } catch (e) {
        it && (typeof it!.throw === 'function') && (await it!.throw!(e));
    }
}

export async function* fromReadableDOMStream<T extends TElement>(source: ReadableStream<T>): AsyncIterableIterator<Uint8Array> {

    let done = false, value: T;
    let bytesToRead = -1, bufferLength = 0;
    let it: ReadableStreamDefaultReader<T> | null = null;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | null = null;
    const slice = () => ((
        [buffer, buffers] = concat(buffers, bytesToRead)) &&
        (bufferLength -= buffer.byteLength) && buffer || buffer);

    try {
        do {
            // If we have enough bytes, concat the chunks and emit the buffer.
            // On the first iteration, this allows the caller to inject bytesToRead
            // before establishing the ReadableStream lock.
            (bytesToRead && (bytesToRead <= bufferLength)) &&
                            (bytesToRead = yield slice());
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
            if (done) while ((bytesToRead = yield slice()) < bufferLength);
        } while (!done);
    } finally {
        try { it && source['locked'] && it['releaseLock'](); } catch (e) {}
    }
}

export async function* fromReadableNodeStream(stream: NodeJS.ReadableStream): AsyncIterableIterator<Uint8Array> {

    type EventName = 'end' | 'error' | 'readable';
    type Event = [EventName, (_: any) => void, Promise<[EventName, Error | null]>];

    const onEvent = <T extends string>(event: T) => {
        let handler = (_: any) => resolve([event, _]);
        let resolve: (value?: [T, any] | PromiseLike<[T, any]>) => void;
        return [event, handler, new Promise<[T, any]>(
            (r) => (resolve = r) && stream.once(event, handler)
        )] as Event;
    };
    
    let bytesToRead = -1, bufferLength = 0;
    let event: EventName, events: Event[] = [];
    let done = false, err: Error | null = null;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | Buffer | string;
    const slice = () => ((
        [buffer, buffers] = concat(buffers, bytesToRead)) &&
        (bufferLength -= buffer.byteLength) && buffer || buffer);

    try {
        do {
            // If we have enough bytes, concat the chunks and emit the buffer.
            // On the first iteration, this allows the caller to inject bytesToRead
            // before listening to the source stream's 'readable' event.
            (bytesToRead && (bytesToRead <= bufferLength)) &&
                            (bytesToRead = yield slice());

            // initialize the stream event handlers
            (events[0] || (events[0] = onEvent('end')));
            (events[1] || (events[1] = onEvent('error')));
            (events[2] || (events[2] = onEvent('readable')));

            // wait on the first message event from the stream
            [event, err] = await Promise.race(events.map((x) => x[2]));

            buffer = toUint8Array(stream.read(bytesToRead));
            // if chunk is null or empty, wait for the next readable event
            if (!buffer || !(buffer.byteLength > 0)) {
                events[3] = onEvent('readable');
            } else {
                // otherwise push it onto the queue
                buffers.push(buffer);
                bufferLength += buffer.byteLength;
            }
            // if the stream emitted an Error, rethrow it
            if (event === 'error') throw err;
            // otherwise if we're not done, continue;
            if (!(done = (event === 'end'))) continue;
            // if we're done, emit the rest of the chunks
            while ((bytesToRead = yield slice()) < bufferLength);
        } while (!done);
    } catch (e) {
        if (err == null) {
            throw (err = await cleanup(events, e != null ? e : new Error()));
        }
    } finally { (err == null) && (await cleanup(events)); }

    function cleanup<T extends Error | null | void>(events: (Event | null)[], err?: T) {
        return new Promise<T>((resolve, reject) => {
            for (let ev, func; events.length > 0; ([ev, func] = events.pop()!) && stream.off(ev, func));
            const destroyStream = (stream as any).destroy || ((err: T, callback: any) => callback(err));
            destroyStream.call(stream, err == null ? null : err, (e: T) => e == null ? reject(e) : resolve(err));
        });
    }
}

export function toReadableStream(source: AsyncIterable<Uint8Array>) {

    let done = false, value: Uint8Array, it: AsyncIterator<Uint8Array>;

    const close = async (method: 'throw' | 'return', value?: any) =>
        (it && (typeof it[method] === 'function') &&
            ({ done } = await it[method]!(value)) ||
        (done = true)) && (it = null as any);

    return new ReadableStream<Uint8Array>({
        cancel: close.bind(null, 'return'),
        async start() { it = source[Symbol.asyncIterator](); },
        async pull(c) {
            try {
                while (!done) {
                    ({ done, value } = await it.next(c.desiredSize));
                    if (value && value.length > 0) return c.enqueue(value);
                }
                return (await close('return')) || c.close();
            } catch (e) { (await close('throw', e)) || c.error(e); }
        }
    });
}

export function toNodeStream(source: AsyncIterable<Uint8Array>) {

    let done = false, value: Uint8Array, it: AsyncIterator<Uint8Array>;

    const close = async (signal: 'throw' | 'return', value?: any) =>
        (it && (typeof it[signal] === 'function') &&
            ({ done } = await it[signal]!(value)) ||
        (done = true)) && (it = null as any);

    const read = (sink: NodeStream, size?: number): Promise<boolean> =>
        done ? end(sink) : it.next(size).then((x) =>
            ({ done, value } = x) && !(value.byteLength > 0) ?
                read(sink, size) : sink.push(x) && read(sink));

    const end = (sink: NodeStream) => (sink.push(null) || true) && close('return');

    return new NodeStream({
        read(size?: number) {
            (it || (it = source[Symbol.asyncIterator]())) &&
                read(this, size);
        },
        destroy(e: Error | null, cb: (e: Error | null) => void) {
            const signal = e == null ? 'return' : 'throw';
            close(signal, e).then(() => cb(null), cb);
        }
    });
}
