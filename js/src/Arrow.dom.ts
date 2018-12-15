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

import { DataType } from './type';
import streamAdapters from './io/adapters';
import { RecordBatch } from './recordbatch';
import { RecordBatchReader } from './ipc/reader';
import { RecordBatchWriter } from './ipc/writer';
import { ReadableDOMStreamOptions } from './io/interfaces';
import { isIterable, isAsyncIterable } from './util/compat';
import { AsyncByteStream, AsyncByteQueue } from './io/stream';

streamAdapters.toReadableDOMStream = toReadableDOMStream;
RecordBatchReader.throughDOM = recordBatchReaderThroughDOMStream;
RecordBatchWriter.throughDOM = recordBatchWriterThroughDOMStream;

export * from './Arrow';

function recordBatchReaderThroughDOMStream<T extends { [key: string]: DataType } = any>() {

    const through = new AsyncByteQueue();
    let reader: RecordBatchReader<T> | null = null;

    const readable = new ReadableStream<RecordBatch<T>>({
        async cancel() { await through.close(); },
        async start(controller) { await next(controller, reader || (reader = await open())); },
        async pull(controller) { reader ? await next(controller, reader) : controller.close(); }
    });

    return { writable: new WritableStream(through), readable };

    async function open() {
        return await (await RecordBatchReader.from(through)).open();
    }

    async function next(controller: ReadableStreamDefaultController<RecordBatch<T>>, reader: RecordBatchReader<T>) {
        let size = controller.desiredSize;
        let r: IteratorResult<RecordBatch<T>> | null = null;
        while (!(r = await reader.next()).done) {
            controller.enqueue(r.value);
            if (size != null && --size <= 0) {
                return;
            }
        }
        controller.close();
    }
}

function recordBatchWriterThroughDOMStream<T extends { [key: string]: DataType } = any>(
    this: typeof RecordBatchWriter,
    writableStrategy?: QueuingStrategy<RecordBatch<T>>,
    readableStrategy?: { highWaterMark?: number, size?: any }
) {

    const through = new AsyncByteQueue();
    const writer = new this<T>().reset(through);
    const reader = new AsyncByteStream(through);
    const readable = new ReadableStream({
        type: 'bytes',
        async cancel() { await through.close(); },
        async pull(controller) { await next(controller); },
        async start(controller) { await next(controller); },
    }, readableStrategy);

    return { writable: new WritableStream(writer, writableStrategy), readable };

    async function next(controller: ReadableStreamDefaultController<Uint8Array>) {
        let buf: Uint8Array | null = null;
        let size = controller.desiredSize;
        while (buf = await reader.read(size || null)) {
            // Work around https://github.com/whatwg/streams/blob/0ebe4b042e467d9876d80ae045de3843092ad797/reference-implementation/lib/helpers.js#L126
            controller.enqueue((buf.buffer.byteLength !== 0) ? buf : buf.slice());
            if (size != null && (size -= buf.byteLength) <= 0) {
                return;
            }
        }
        controller.close();
    }
}

function toReadableDOMStream<T>(source: Iterable<T> | AsyncIterable<T>, options?: ReadableDOMStreamOptions): ReadableStream<T> {
    if (isAsyncIterable<T>(source)) { return asyncIterableAsReadableDOMStream(source, options); }
    if (isIterable<T>(source)) { return iterableAsReadableDOMStream(source, options); }
    throw new Error(`toReadableDOMStream() must be called with an Iterable or AsyncIterable`);
}

function iterableAsReadableDOMStream<T>(source: Iterable<T>, options?: ReadableDOMStreamOptions) {

    let it: Iterator<T> | null = null;

    return new ReadableStream<T>({
        ...options as any,
        start(controller) { next(controller, it || (it = source[Symbol.iterator]())); },
        pull(controller) { it ? (next(controller, it)) : controller.close(); },
        cancel() { (it && (it.return && it.return()) || true) && (it = null); }
    });

    function next(controller: ReadableStreamDefaultController<T>, it: Iterator<T>) {
        let size = controller.desiredSize;
        let r: IteratorResult<T> | null = null;
        while ((size == null || size-- > 0) && !(r = it.next()).done) {
            controller.enqueue(r.value);
        }
        r && r.done && controller.close();
    }
}

function asyncIterableAsReadableDOMStream<T>(source: AsyncIterable<T>, options?: ReadableDOMStreamOptions) {

    let it: AsyncIterator<T> | null = null;

    return new ReadableStream<T>({
        ...options as any,
        async start(controller) { await next(controller, it || (it = source[Symbol.asyncIterator]())); },
        async pull(controller) { it ? (await next(controller, it)) : controller.close(); },
        async cancel() { (it && (it.return && await it.return()) || true) && (it = null); },
    });

    async function next(controller: ReadableStreamDefaultController<T>, it: AsyncIterator<T>) {
        let size = controller.desiredSize;
        let r: IteratorResult<T> | null = null;
        while ((size == null || size-- > 0) && !(r = await it.next()).done) {
            controller.enqueue(r.value);
        }
        r && r.done && controller.close();
    }
}
