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

import { Readable } from 'stream';
import { concat, toUint8Array } from '../util/buffer';

export function fromReadableNodeStream(stream: NodeJS.ReadableStream): AsyncIterableIterator<Uint8Array> {
    const pumped = _fromReadableNodeStream(stream);
    pumped.next();
    return pumped;
}

export function toReadableNodeStream(source: Iterable<Uint8Array> | AsyncIterable<Uint8Array>) {

    let done = false, value: Uint8Array;
    let it: Iterator<Uint8Array> | AsyncIterator<Uint8Array>;

    const close = async (signal: 'throw' | 'return', value?: any) =>
        (it && (typeof it[signal] === 'function') &&
            ({ done } = await it[signal]!(value)) ||
        (done = true)) && (it = null as any);

    const read = (sink: Readable, size?: number): Promise<boolean> =>
        done ? end(sink) : Promise.resolve(it.next(size)).then((x) =>
            ({ done, value } = x) && !(value.byteLength > 0) ?
                read(sink, size) : sink.push(x) && read(sink));

    const end = (sink: Readable) => (sink.push(null) || true) && close('return');
    const createIterator = () => {
        const xs: any = source;
        const fn = xs[Symbol.asyncIterator] || xs[Symbol.iterator];
        return fn.call(source);
    };

    return new Readable({
        read(size?: number) {
            (it || (it = createIterator())) && read(this, size);
        },
        destroy(e: Error | null, cb: (e: Error | null) => void) {
            const signal = e == null ? 'return' : 'throw';
            close(signal, e).then(() => cb(null), cb);
        }
    });
}

async function* _fromReadableNodeStream(stream: NodeJS.ReadableStream): AsyncIterableIterator<Uint8Array> {

    type EventName = 'end' | 'error' | 'readable';
    type Event = [EventName, (_: any) => void, Promise<[EventName, Error | null]>];

    const onEvent = <T extends string>(event: T) => {
        let handler = (_: any) => resolve([event, _]);
        let resolve: (value?: [T, any] | PromiseLike<[T, any]>) => void;
        return [event, handler, new Promise<[T, any]>(
            (r) => (resolve = r) && stream.once(event, handler)
        )] as Event;
    };

    let event: EventName, events: Event[] = [];
    let done = false, err: Error | null = null;
    // Yield so the caller can inject bytesToRead before we add the
    // listener for the source stream's 'readable' event.
    let bytesToRead = yield <any> null, bufferLength = 0;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | Buffer | string;
    const slice = () => ((
        [buffer, buffers] = concat(buffers, bytesToRead)) &&
        (bufferLength -= buffer.byteLength) && buffer || buffer);

    try {
        do {
            // If we have enough bytes, concat the chunks and emit the buffer.
            (bytesToRead <= bufferLength) && (bytesToRead = yield slice());

            // initialize the stream event handlers
            (events[0] || (events[0] = onEvent('end')));
            (events[1] || (events[1] = onEvent('error')));
            (events[2] || (events[2] = onEvent('readable')));

            // wait on the first message event from the stream
            [event, err] = await Promise.race(events.map((x) => x[2]));

            buffer = toUint8Array(stream.read(bytesToRead));
            // if chunk is null or empty, wait for the next readable event
            if (!buffer || !(buffer.byteLength > 0)) {
                events[2] = onEvent('readable');
            } else {
                // otherwise push it onto the queue
                buffers.push(buffer);
                bufferLength += buffer.byteLength;
            }
            // if the stream emitted an Error, rethrow it
            if (event === 'error') { throw err; }
            // if we're done, emit the rest of the chunks
            if (done = (event === 'end')) {
                do {
                    bytesToRead = yield slice();
                } while (bytesToRead < bufferLength);
            }
        } while (!done);
    } catch (e) {
        if (err == null) {
            throw (err = await cleanup(events, e != null ? e : new Error()));
        }
    } finally { (err == null) && (await cleanup(events)); }

    function cleanup<T extends Error | null | void>(events: Event[], err?: T) {
        return new Promise<T>((resolve, reject) => {
            while (events.length > 0) {
                let eventAndHandler = events.pop();
                if (eventAndHandler) {
                    let [ev, fn] = eventAndHandler;
                    ev && fn && stream.off(ev, fn);
                }
            }
            const destroyStream = (stream as any).destroy || ((err: T, callback: any) => callback(err));
            destroyStream.call(stream, err, (e: T) => e == null ? reject(e) : resolve(err));
        });
    }
}
