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
import { Duplex, Readable } from 'stream';
import streamAdapters from './io/adapters';
import { RecordBatchReader } from './ipc/reader';
import { RecordBatchWriter } from './ipc/writer';
import { isIterable, isAsyncIterable } from './util/compat';
import { AsyncByteStream, AsyncByteQueue } from './io/stream';
import { RecordBatch } from './recordbatch';

type ReadableOptions = import('stream').ReadableOptions;

streamAdapters.toReadableNodeStream = toReadableNodeStream;
RecordBatchReader['throughNode'] = recordBatchReaderThroughNodeStream;
RecordBatchWriter['throughNode'] = recordBatchWriterThroughNodeStream;

export * from './Arrow.dom';

function recordBatchReaderThroughNodeStream<T extends { [key: string]: DataType } = any>() {

    let reading = false;
    let reader: RecordBatchReader<T> | null = null;
    let through = new AsyncByteQueue() as AsyncByteQueue | null;

    return new Duplex({
        allowHalfOpen: false,
        readableObjectMode: true,
        writableObjectMode: false,
        final(cb) { through && through.close(); cb(); },
        write(x, _, cb) { through && through.write(x); cb(); },
        read(size: number): void {
            through && (reading || (reading = !!(async () =>
                await next(this, size, reader || (reader = await open(through)))
            )()));
        },
        destroy(err, cb) {
            reading = true;
            through && (err ? through.abort(err) : through.close());
            cb(reader = through = null);
        }
    });

    async function open(queue: AsyncByteQueue) {
        return await (await RecordBatchReader.from(queue)).open();
    }

    async function next(sink: Readable, size: number, reader: RecordBatchReader<T>): Promise<any> {
        let r: IteratorResult<RecordBatch<T>> | null = null;
        while (sink.readable && !(r = await reader.next()).done) {
            if (!sink.push(r.value) || (size != null && --size <= 0)) {
                return reading = false;
            }
        }
        sink.push(null);
        await reader.cancel();
    }
}

function recordBatchWriterThroughNodeStream<T extends { [key: string]: DataType } = any>(this: typeof RecordBatchWriter) {

    let reading = false;
    let through: AsyncByteQueue | null = new AsyncByteQueue();
    let reader: AsyncByteStream | null = new AsyncByteStream(through);
    let writer: RecordBatchWriter<T> | null = new this<T>().reset(through);

    return new Duplex({
        allowHalfOpen: false,
        writableObjectMode: true,
        readableObjectMode: false,
        final(cb) { writer && writer.close(); cb(); },
        write(x, _, cb) { writer && writer.write(x); cb(); },
        read(size: number): void {
            reader && (reading || (reading = !!(async () =>
                await next(this, size, reader)
            )()));
        },
        destroy(err, cb) {
            reading = true;
            writer && (err ? writer.abort(err) : writer.close());
            cb(through = reader = writer = null);
        }
    });

    async function next(sink: Readable, size: number, reader: AsyncByteStream): Promise<any> {
        let buf: Uint8Array | null = null;
        while (sink.readable && (buf = await reader.read())) {
            if (!sink.push(buf) || (size != null && (size -= buf.byteLength) <= 0)) {
                return reading = false;
            }
        }
        sink.push(null);
        await reader.cancel();
    }
}

function toReadableNodeStream<T>(source: Iterable<T> | AsyncIterable<T>, options?: ReadableOptions): Readable {
    if (isAsyncIterable<T>(source)) { return asyncIterableAsReadableNodeStream(source, options); }
    if (isIterable<T>(source)) { return iterableAsReadableNodeStream(source, options); }
    throw new Error(`toReadableNodeStream() must be called with an Iterable or AsyncIterable`);
}

function iterableAsReadableNodeStream<T>(source: Iterable<T>, options?: ReadableOptions) {
    let it: Iterator<T>, reading = false;
    return new Readable({
        ...options,
        read(size: number) {
            !reading && (reading = true) &&
                next(this, size, (it || (it = source[Symbol.iterator]())));
        },
        destroy(e: Error | null, cb: (e: Error | null) => void) {
            if ((reading = true) && it || Boolean(cb(null))) {
                let fn = e == null ? it.return : it.throw;
                (fn && fn.call(it, e) || true) && cb(null);
            }
        },
    });
    function next(sink: Readable, size: number, it: Iterator<T>): any {
        let r: IteratorResult<T> | null = null;
        while (sink.readable && (size == null || size-- > 0) && !(r = it.next()).done) {
            if (!sink.push(r.value)) { return reading = false; }
        }
        if (((r && r.done) || !sink.readable) && (reading = sink.push(null) || true)) {
            it.return && it.return();
        }
    }
}

function asyncIterableAsReadableNodeStream<T>(source: AsyncIterable<T>, options?: ReadableOptions) {
    let it: AsyncIterator<T>, reading = false;
    return new Readable({
        ...options,
        read(size: number) {
            reading || (reading = !!(async () => (
                await next(this, size, (it || (it = source[Symbol.asyncIterator]())))
            ))());
        },
        destroy(e: Error | null, cb: (e: Error | null) => void) {
            if ((reading = true) && it || Boolean(cb(null))) {
                (async (fn) => {
                    (fn && await fn.call(it, e) || true) && cb(null)
                })(e == null ? it.return : it.throw);
            }
        },
    });
    async function next(sink: Readable, size: number, it: AsyncIterator<T>): Promise<any> {
        let r: IteratorResult<T> | null = null;
        while (sink.readable && (size == null || size-- > 0) && !(r = await it.next()).done) {
            if (!sink.push(r.value)) { return reading = false; }
        }
        if (((r && r.done) || !sink.readable) && (reading = sink.push(null) || true)) {
            it.return && await it.return();
        }
    }
}
