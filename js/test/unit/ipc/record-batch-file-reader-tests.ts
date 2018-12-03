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
import { nodeToDOMStream, readableDOMStreamToAsyncIterator } from './util';

import { Schema } from '../../../src/schema';
import { RecordBatch } from '../../../src/recordbatch';
import { RecordBatchReader }  from '../../../src/ipc/reader';
import { AsyncRecordBatchReader } from '../../../src/ipc/reader/base';
import { RecordBatchFileReader } from '../../../src/ipc/reader/file';
import { RecordBatchStreamReader } from '../../../src/ipc/reader/stream';

const simpleFilePath = Path.resolve(__dirname, `../../data/cpp/file/simple.arrow`);
const simpleFileData = fs.readFileSync(simpleFilePath);

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

describe('RecordBatchFileReader', () => {

    it('should read all messages from a Buffer of Arrow data in memory', () => {
        testSimpleRecordBatchFileReader(RecordBatchReader.open(simpleFileData));
    });

    it('should read all messages from an Iterable that yields buffers of Arrow messages in memory', () => {
        testSimpleRecordBatchFileReader(RecordBatchReader.open(iterableSource(simpleFileData)));
    });

    describe('asReadableDOMStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = simpleFileData;
            const reader = RecordBatchReader.open(source);
            const iterator = readableDOMStreamToAsyncIterator(reader.asReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('asReadableNodeStream', () => {
        it('should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.open(simpleFileData);
            const iterator = reader.asReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    function testSimpleRecordBatchFileReader(reader: RecordBatchFileReader | RecordBatchStreamReader) {

        let r: IteratorResult<RecordBatch>;

        reader = reader.open();
        expect(reader.isFile()).toBe(true);
        expect(reader.readSchema()).toBeInstanceOf(Schema);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(RecordBatch);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(RecordBatch);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(RecordBatch);

        expect(reader.next().done).toBe(true);

        reader.return();
    }
});

describe('AsyncRecordBatchFileReader', () => {
    
    it('should read all messages from a NodeJS ReadableStream', async () => {
        const source = fs.createReadStream(simpleFilePath);
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.open(source));
    });

    it('should read all messages from a NodeJS Promise<FileHandle>', async () => {
        const source = fs.promises.open(simpleFilePath, 'r');
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.open(source));
    });

    it('should read all messages from a NodeJS FileHandle', async () => {
        const source = await fs.promises.open(simpleFilePath, 'r');
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.open(source));
    });

    it('should read all messages from an AsyncIterable that yields buffers of Arrow messages in memory', async () => {
        const source = asyncIterableSource(simpleFileData);
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.open(source));
    });

    it('should read all messages from a whatwg ReadableStream', async () => {
        const source = nodeToDOMStream(fs.createReadStream(simpleFilePath));
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.open(source));
    });

    it('should read all messages from a whatwg ReadableByteStream', async () => {
        const source = nodeToDOMStream(fs.createReadStream(simpleFilePath), { type: 'bytes' });
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.open(source));
    });

    describe('asReadableDOMStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = fs.createReadStream(simpleFilePath);
            const reader = await RecordBatchReader.open(source);
            const iterator = readableDOMStreamToAsyncIterator(reader.asReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('asReadableNodeStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = fs.createReadStream(simpleFilePath);
            const reader = await RecordBatchReader.open(source);
            const iterator = reader.asReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    async function testSimpleAsyncRecordBatchFileReader(reader: RecordBatchFileReader | AsyncRecordBatchReader) {
        reader = await reader.open();
        expect(reader.isFile()).toBe(true);
        expect(await reader.readSchema()).toBeInstanceOf(Schema);
        await testSimpleAsyncRecordBatchIterator(reader);
    }
});

async function testSimpleAsyncRecordBatchIterator(iterator: Iterator<RecordBatch> | AsyncIterator<RecordBatch>) {

    let r: IteratorResult<RecordBatch>;

    r = await iterator.next();
    expect(r.done).toBe(false);
    expect(r.value).toBeInstanceOf(RecordBatch);

    r = await iterator.next();
    expect(r.done).toBe(false);
    expect(r.value).toBeInstanceOf(RecordBatch);

    r = await iterator.next();
    expect(r.done).toBe(false);
    expect(r.value).toBeInstanceOf(RecordBatch);

    expect((await iterator.next()).done).toBe(true);

    await iterator.return!();
}
