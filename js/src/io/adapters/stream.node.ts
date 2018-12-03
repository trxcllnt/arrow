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

import { joinUint8Arrays, toUint8Array } from '../../util/buffer';

type EventName = 'end' | 'error' | 'readable';
type Event = [EventName, (_: any) => void, Promise<[EventName, Error | null]>];
const pump = <T extends Iterator<any> | AsyncIterator<any>>(iterator: T) => { iterator.next(); return iterator; };
const onEvent = <T extends string>(stream: NodeJS.ReadableStream, event: T) => {
    let handler = (_: any) => resolve([event, _]);
    let resolve: (value?: [T, any] | PromiseLike<[T, any]>) => void;
    return [event, handler, new Promise<[T, any]>(
        (r) => (resolve = r) && stream.once(event, handler)
    )] as Event;
};

/**
 * @ignore
 */
export function fromReadableNodeStream(stream: NodeJS.ReadableStream): AsyncIterableIterator<Uint8Array> {
    return pump(_fromReadableNodeStream(stream));
}

async function* _fromReadableNodeStream(stream: NodeJS.ReadableStream): AsyncIterableIterator<Uint8Array> {

    let events: Event[] = [];
    let event: EventName = 'error';
    let done = false, err: Error | null = null;
    let cmd: 'peek' | 'read', size: number, bufferLength = 0;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | Buffer | string;

    function byteRange() {
        if (cmd === 'peek') {
            return joinUint8Arrays(buffers.slice(), size)[0];
        }
        [buffer, buffers] = joinUint8Arrays(buffers, size);
        bufferLength -= buffer.byteLength;
        return buffer;
    }

    // Yield so the caller can inject the read command before we
    // add the listener for the source stream's 'readable' event.
    ({ cmd, size } = yield <any> null);

    try {
        // initialize the stream event handlers
        events[0] = onEvent(stream, 'end');
        events[1] = onEvent(stream, 'error');

        do {
            events[2] = onEvent(stream, 'readable');

            // wait on the first message event from the stream
            [event, err] = await Promise.race(events.map((x) => x[2]));

            // if the stream emitted an Error, rethrow it
            if (event === 'error') { throw err; }
            if (!(done = event === 'end')) {
                buffer = isNaN(size - bufferLength)
                    ? toUint8Array(stream.read(undefined))
                    : toUint8Array(stream.read(size - bufferLength));
                // if chunk is not null or empty, push it onto the queue
                if (buffer && buffer.byteLength > 0) {
                    buffers.push(buffer);
                    bufferLength += buffer.byteLength;
                }
            }
            // If we have enough bytes in our buffer, yield chunks until we don't
            if (done || size <= bufferLength) {
                do {
                    ({ cmd, size } = yield byteRange());
                } while (size < bufferLength);
            }
        } while (!done);
    } catch (e) {
        throw (err = await cleanup(events, event === 'error' ? err : e));
    } finally { (err == null) && (await cleanup(events, err)); }

    function cleanup<T extends Error | null | void>(events: Event[], err?: T) {
        buffer = buffers = <any> null;
        return new Promise<T>((resolve, reject) => {
            while (events.length > 0) {
                const [ev, fn] = events.pop() || []!;
                if (ev && fn) { stream.off(ev, fn); }
            }
            const destroy = (stream as any).destroy || ((err: T, cb: any) => cb(err));
            destroy.call(stream, err, (e: T) => e != null ? reject(e) : resolve(err));
        });
    }
}
