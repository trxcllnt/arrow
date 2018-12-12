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

import { Readable, PassThrough } from 'stream';
import { toUint8Array } from '../../../src/util/buffer';

export function* chunkedIterable(buffer: Uint8Array) {
    let offset = 0, size = 0;
    while (offset < buffer.byteLength) {
        size = yield buffer.subarray(offset, offset +=
            (isNaN(+size) ? buffer.byteLength - offset : size));
    }
}

export async function* asyncChunkedIterable(buffer: Uint8Array) {
    let offset = 0, size = 0;
    while (offset < buffer.byteLength) {
        size = yield buffer.subarray(offset, offset +=
            (isNaN(+size) ? buffer.byteLength - offset : size));
    }
}

export async function concatBuffersAsync(iterator: AsyncIterable<Uint8Array>) {
    let chunks = [], total = 0;
    for await (const chunk of iterator) {
        chunks.push(chunk);
        total += chunk.byteLength;
    }
    return chunks.reduce((x, buffer) => {
        x.buffer.set(buffer, x.offset);
        x.offset += buffer.byteLength;
        return x;
    }, { offset: 0, buffer: new Uint8Array(total) }).buffer;
}

export async function* readableDOMStreamToAsyncIterator<T>(stream: ReadableStream<T>) {
    // Get a lock on the stream
    const reader = stream.getReader();
    try {
        while (true) {
            // Read from the stream
            const { done, value } = await reader.read();
            // Exit if we're done
            if (done) { break; }
            // Else yield the chunk
            yield value as T;
        }
    } catch (e) {
        throw e;
    } finally {
        try { stream.locked && reader.releaseLock(); } catch (e) {}
    }
}

export function nodeToDOMStream(stream: NodeJS.ReadableStream, opts: any = {}) {
    stream = new Readable().wrap(stream);
    return new ReadableStream({
        ...opts,
        start(controller) {
            stream.pause();
            stream.on('data', (chunk) => {
                // We have to copy the chunk bytes here because the whatwg ReadableByteStream reference
                // implementation redefines the `byteLength` property of the underlying ArrayBuffers that
                // pass through it, to mimic the (as yet specified) ArrayBuffer "transfer" semantics.
                // Node allocates ArrayBuffers in 64k chunks, and shares them between unrelated active
                // file streams.
                // When the ReadableByteStream sets the ArrayBuffer's `byteLength` to 0, it isn't just
                // setting this chunk's ArrayBuffer byteLength to zero, but rather the ArrayBuffer for
                // all the currently active file streams. See the code here for more details:
                // https://github.com/whatwg/streams/blob/0ebe4b042e467d9876d80ae045de3843092ad797/reference-implementation/lib/helpers.js#L126
                controller.enqueue(toUint8Array(chunk).slice());
                stream.pause();
            });
            stream.on('end', () => controller.close());
            stream.on('error', e => controller.error(e));
        },
        pull() { stream.resume(); },
        cancel(reason) {
            stream.pause();
            if (typeof (stream as any).cancel === 'function') {
                return (stream as any).cancel(reason);
            } else if (typeof (stream as any).destroy === 'function') {
                return (stream as any).destroy(reason);
            }
        }
    });
}

export function convertNodeToDOMStream() {
    const stream = new PassThrough();
    (stream as any).dom = nodeToDOMStream(stream);
    return stream as PassThrough & { dom: ReadableStream<any> };
}
