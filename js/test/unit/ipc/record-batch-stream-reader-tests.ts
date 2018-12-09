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

import '../../Arrow';
import * as fs from 'fs';
import * as Path from 'path';
import { toArray } from 'ix/asynciterable/toarray';
import { nodeToDOMStream, readableDOMStreamToAsyncIterator } from './util';

import { Schema } from '../../../src/schema';
import { RecordBatch } from '../../../src/recordbatch';
import { RecordBatchReader }  from '../../../src/ipc/reader';
import { RecordBatchFileReader } from '../../../src/ipc/reader';
import { RecordBatchStreamReader } from '../../../src/ipc/reader';
import { AsyncRecordBatchStreamReader } from '../../../src/ipc/reader';

const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
const simpleStreamData = fs.readFileSync(simpleStreamPath);

function* iterableSource(buffer: Uint8Array) {
    let offset = 0, size = 0;
    while (offset < buffer.byteLength) {
        size = yield buffer.subarray(offset, offset +=
            (isNaN(+size) ? buffer.byteLength - offset : size));
    }
}

async function* asyncIterableSource(buffer: Uint8Array) {
    let offset = 0, size = 0;
    while (offset < buffer.byteLength) {
        size = yield buffer.subarray(offset, offset +=
            (isNaN(+size) ? buffer.byteLength - offset : size));
    }
}

describe('RecordBatchStreamReader', () => {

    it('should read all messages from a Buffer of Arrow data in memory', () => {
        const source = simpleStreamData;
        testSimpleRecordBatchStreamReader(RecordBatchReader.from(source));
    });

    it('should read all messages from an Iterable that yields buffers of Arrow messages in memory', () => {
        const source = iterableSource(simpleStreamData);
        testSimpleRecordBatchStreamReader(RecordBatchReader.from(source));
    });

    it('should read all messages from an Iterable that yields multiple tables as buffers of Arrow messages in memory', () => {

        const source = (function *() {
            yield* iterableSource(simpleStreamData);
            yield* iterableSource(simpleStreamData);
        }());

        let reader = RecordBatchReader.from(source);
        let schema = reader.open(false).schema;

        reader = testSimpleRecordBatchStreamReader(reader);
        expect(reader.schema).toBe(schema);
        expect(reader.reset().schema).toBeUndefined();

        reader = testSimpleRecordBatchStreamReader(reader);
        expect(reader.close().schema).toBeUndefined();
        expect(reader.open().schema).toBeUndefined();
    });

    describe('asReadableDOMStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = simpleStreamData;
            const reader = RecordBatchReader.from(source);
            const iterator = readableDOMStreamToAsyncIterator(reader.asReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('asReadableNodeStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = simpleStreamData;
            const reader = RecordBatchReader.from(source);
            const iterator = reader.asReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    function testSimpleRecordBatchStreamReader(reader: RecordBatchFileReader | RecordBatchStreamReader) {
        reader = reader.open();
        expect(reader.isStream()).toBe(true);
        expect(reader.schema).toBeInstanceOf(Schema);
        const batches = [...reader];
        expect(batches.length).toEqual(3);
        batches.forEach((b, i) => {
            try {
                expect(b).toBeInstanceOf(RecordBatch);
            } catch (e) { throw new Error(`${i}: ${e}`); }
        });
        return reader;
    }
});

describe('AsyncRecordBatchStreamReader', () => {

    it('should read all messages from a NodeJS ReadableStream', async () => {
        const source = fs.createReadStream(simpleStreamPath);
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from(source));
    });

    it('should read all messages from an AsyncIterable that yields buffers of Arrow messages in memory', async () => {
        const source = asyncIterableSource(simpleStreamData);
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from(source));
    });

    it('should read all messages from a whatwg ReadableStream', async () => {
        const source = nodeToDOMStream(fs.createReadStream(simpleStreamPath));
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from(source));
    });

    it('should read all messages from a whatwg ReadableByteStream', async () => {
        const source = nodeToDOMStream(fs.createReadStream(simpleStreamPath), { type: 'bytes' });
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from(source));
    });

    it('should read all messages from an AsyncIterable that yields multiple tables as buffers of Arrow messages in memory', async () => {

        const source = (async function *() {
            yield* asyncIterableSource(simpleStreamData);
            yield* asyncIterableSource(simpleStreamData);
        }());

        let reader = await RecordBatchReader.from(source);
        let schema = (await reader.open(false)).schema;

        reader = await testSimpleAsyncRecordBatchStreamReader(reader);
        expect(reader.schema).toBe(schema);
        expect(reader.reset().schema).toBeUndefined();

        reader = await testSimpleAsyncRecordBatchStreamReader(reader);
        expect((await reader.close()).schema).toBeUndefined();
        expect((await reader.open()).schema).toBeUndefined();
    });

    describe('asReadableDOMStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = asyncIterableSource(simpleStreamData);
            const reader = await RecordBatchReader.from(source);
            const iterator = readableDOMStreamToAsyncIterator(reader.asReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('asReadableNodeStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = asyncIterableSource(simpleStreamData);
            const reader = await RecordBatchReader.from(source);
            const iterator = reader.asReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('createDOMTransformStream', () => {
        it.only('should transform all messages from a whatwg ReadableStream', async () => {
            const source = nodeToDOMStream(fs.createReadStream(simpleStreamPath));
            const batches = source.pipeThrough(RecordBatchReader.createDOMTransformStream());
            await testSimpleAsyncRecordBatchIterator(readableDOMStreamToAsyncIterator(batches));
        });
    });

    async function testSimpleAsyncRecordBatchStreamReader(reader: RecordBatchFileReader | AsyncRecordBatchStreamReader) {
        reader = await reader.open();
        expect(reader.isStream()).toBe(true);
        expect(reader.schema).toBeInstanceOf(Schema);
        await testSimpleAsyncRecordBatchIterator(reader as AsyncIterableIterator<RecordBatch>);
        return reader;
    }
});

async function testSimpleAsyncRecordBatchIterator(iterator: AsyncIterableIterator<RecordBatch>) {
    const batches = await toArray(iterator);
    expect(batches.length).toEqual(3);
    batches.forEach((b, i) => {
        try {
            expect(b).toBeInstanceOf(RecordBatch);
        } catch (e) { throw new Error(`${i}: ${e}`); }
    });
    await iterator.return!();
}
