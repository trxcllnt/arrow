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

import { ReadableDOMStream } from '../interfaces';
import { joinUint8Arrays, toUint8Array } from '../../util/buffer';

type TElement = ArrayBufferLike | ArrayBufferView | string;
type ReadResult<T = any> = import('whatwg-streams').ReadResult<T>;
type ReadableStreamBYOBReader<T = any> = import('whatwg-streams').ReadableStreamBYOBReader<T>;
type ReadableStreamDefaultReader<T = any> = import('whatwg-streams').ReadableStreamDefaultReader<T>;

const pump = <T extends Iterator<any> | AsyncIterator<any>>(iterator: T) => { iterator.next(); return iterator; }

/**
 * @ignore
 */
export function fromReadableDOMStream<T extends TElement>(source: ReadableDOMStream<T>): AsyncIterableIterator<Uint8Array> {
    return pump(_fromReadableDOMStream<T>(source));
}

// All this manual Uint8Array chunk management can be avoided if/when engines
// add support for ArrayBuffer.transfer() or ArrayBuffer.prototype.realloc():
// https://github.com/domenic/proposal-arraybuffer-transfer
async function* _fromReadableDOMStream<T extends TElement>(source: ReadableDOMStream<T>): AsyncIterableIterator<Uint8Array> {

    let done = false;
    let cmd: 'peek' | 'read', size: number, bufferLength = 0;
    let buffers: Uint8Array[] = [], buffer: Uint8Array | null = null;

    function byteRange() {
        if (cmd === 'peek') {
            return joinUint8Arrays(buffers.slice(), size)[0];
        }
        [buffer, buffers] = joinUint8Arrays(buffers, size);
        bufferLength -= buffer.byteLength;
        return buffer;
    }

    // Yield so the caller can inject the read command before we establish the ReadableStream lock
    ({ cmd, size } = yield <any> null);

    let it: AdaptiveByteReader<T> | null = null;

    try {
        // initialize the reader and lock the stream
        it = new AdaptiveByteReader(source);
        do {
            // read the next value
            ({ done, value: buffer } = isNaN(size - bufferLength)
                ? await it['read'](undefined)
                : await it['read'](size - bufferLength));
            // if chunk is not null or empty, push it onto the queue
            if (buffer && buffer.byteLength > 0) {
                buffers.push(toUint8Array(buffer));
                bufferLength += buffer.byteLength;
            }
            // If we have enough bytes in our buffer, yield chunks until we don't
            if (done || size <= bufferLength) do {
                ({ cmd, size } = yield byteRange());
            } while (size < bufferLength);
        } while (!done);
    } catch (e) {
        source['locked'] && it && (await it!['cancel']());
    } finally { source['locked'] && it && it.releaseLock(); }
}

class AdaptiveByteReader<T extends TElement> {

    private supportsBYOB: boolean;
    private byobReader: ReadableStreamBYOBReader<T> | null = null;
    private defaultReader: ReadableStreamDefaultReader<T> | null = null;
    private reader: ReadableStreamBYOBReader<T> | ReadableStreamDefaultReader<T> | null;

    constructor(private source: ReadableDOMStream<T>) {
        try {
            this.supportsBYOB = !!(this.reader = this.getBYOBReader());
        } catch (e) {
            this.supportsBYOB = !!!(this.reader = this.getDefaultReader());
        }
    }

    get closed(): Promise<void> {
        return this.reader ? this.reader.closed.catch(() => {}) : Promise.resolve();
    }

    releaseLock(): void {
        if (this.reader) {
            this.reader.releaseLock();
        }
        this.reader = this.byobReader = this.defaultReader = null;
    }

    async cancel(reason?: any): Promise<void> {
        const { reader } = this;
        this.reader = null;
        this.releaseLock();
        if (reader) {
            await reader.cancel(reason);
        }
    }

    async read(size?: number): Promise<ReadResult<Uint8Array>> {
        if (size === 0) {
            return { done: this.reader == null, value: new Uint8Array(0) };
        }
        const result = !this.supportsBYOB || typeof size !== 'number'
            ? await this.getDefaultReader().read()
            : await this.readFromBYOBReader(size);
        !result.done && (result.value = toUint8Array(result));
        return result as ReadResult<Uint8Array>;
    }

    private getDefaultReader() {
        if (this.byobReader) { this.releaseLock(); }
        if (!this.defaultReader) {
            this.defaultReader = this.source.getReader();
            // We have to catch and swallow errors here to avoid uncaught promise rejection exceptions
            // that seem to be raised when we call `releaseLock()` on this reader. I'm still mystified
            // about why these errors are raised, but I'm sure there's some important spec reason that
            // I haven't considered. I hate to employ such an anti-pattern here, but it seems like the
            // only solution in this case :/
            this.defaultReader.closed.catch(() => {});
        }
        return (this.reader = this.defaultReader);
    }

    private getBYOBReader() {
        if (this.defaultReader) { this.releaseLock(); }
        if (!this.byobReader) {
            this.byobReader = this.source.getReader({ mode: 'byob' });
            // We have to catch and swallow errors here to avoid uncaught promise rejection exceptions
            // that seem to be raised when we call `releaseLock()` on this reader. I'm still mystified
            // about why these errors are raised, but I'm sure there's some important spec reason that
            // I haven't considered. I hate to employ such an anti-pattern here, but it seems like the
            // only solution in this case :/
            this.byobReader.closed.catch(() => {});
        }
        return (this.reader = this.byobReader);
    }

    // This strategy plucked from the example in the streams spec:
    // https://streams.spec.whatwg.org/#example-manual-read-bytes
    private async readFromBYOBReader(size: number) {
        return await readInto(this.getBYOBReader(), new ArrayBuffer(size), 0);
    }
}

async function readInto<T extends TElement>(reader: ReadableStreamBYOBReader<T>, buffer: ArrayBufferLike, offset: number): Promise<ReadResult<Uint8Array>> {
    const total = buffer.byteLength;
    if (offset >= total) {
        return { done: false, value: new Uint8Array(buffer, 0, total) };
    }
    const r = await reader.read(new Uint8Array(buffer, offset, total - offset));
    r.done && (r.value = new Uint8Array(r.value.buffer, 0, offset + r.value.byteLength));
    return r.done ? r : await readInto(reader, r.value.buffer, offset + r.value.byteLength);
}
