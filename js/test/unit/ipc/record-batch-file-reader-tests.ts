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
import { AsyncRecordBatchFileReader } from '../../../src/ipc/reader';

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
        testSimpleRecordBatchFileReader(RecordBatchReader.from(simpleFileData));
    });

    it('should read all messages from an Iterable that yields buffers of Arrow messages in memory', () => {
        testSimpleRecordBatchFileReader(RecordBatchReader.from(iterableSource(simpleFileData)));
    });

    it('should allow random access to record batches after iterating when autoClose=false', () => {
        const reader = RecordBatchReader.from(simpleFileData) as RecordBatchFileReader;
        const schema = reader.open(false).schema;
        const batches = [...reader];
        expect(reader.closed).toBe(false);
        expect(reader.schema).toBe(schema);
        while (batches.length > 0) {
            const expected = batches.pop()!;
            const actual = reader.readRecordBatch(batches.length);
            expect(actual).toEqualRecordBatch(expected);
        }
        reader.close();
        expect(reader.closed).toBe(true);
        expect(reader.schema).toBeUndefined();
    });

    describe('asReadableDOMStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = simpleFileData;
            const reader = RecordBatchReader.from(source).open();
            const iterator = readableDOMStreamToAsyncIterator(reader.asReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('asReadableNodeStream', () => {
        it('should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.from(simpleFileData).open();
            const iterator = reader.asReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    function testSimpleRecordBatchFileReader(reader: RecordBatchFileReader | RecordBatchStreamReader) {
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

describe('AsyncRecordBatchFileReader', () => {

    it('should read all messages from a NodeJS ReadableStream', async () => {
        const source = fs.createReadStream(simpleFilePath);
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(source));
    });

    it('should read all messages from a NodeJS Promise<FileHandle>', async () => {
        const source = fs.promises.open(simpleFilePath, 'r');
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(source));
    });

    it('should read all messages from a NodeJS FileHandle', async () => {
        const source = await fs.promises.open(simpleFilePath, 'r');
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(source));
    });

    it('should read all messages from an AsyncIterable that yields buffers of Arrow messages in memory', async () => {
        const source = asyncIterableSource(simpleFileData);
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(source));
    });

    it('should read all messages from a whatwg ReadableStream', async () => {
        const source = nodeToDOMStream(fs.createReadStream(simpleFilePath));
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(source));
    });

    it('should read all messages from a whatwg ReadableByteStream', async () => {
        const source = nodeToDOMStream(fs.createReadStream(simpleFilePath), { type: 'bytes' });
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(source));
    });

    it('should allow random access to record batches after iterating when autoClose=false', async () => {
        const source = (await fs.promises.open(simpleFilePath, 'r'));
        const reader = (await RecordBatchReader.from(source)) as AsyncRecordBatchFileReader;
        const schema = (await reader.open(false)).schema;
        const batches = await toArray(reader);
        expect(reader.closed).toBe(false);
        expect(reader.schema).toBe(schema);
        while (batches.length > 0) {
            const expected = batches.pop()!;
            const actual = await reader.readRecordBatch(batches.length);
            expect(actual).toEqualRecordBatch(expected);
        }
        await reader.close();
        expect(reader.closed).toBe(true);
        expect(reader.schema).toBeUndefined();
    });

    describe('asReadableDOMStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = fs.createReadStream(simpleFilePath);
            const reader = await RecordBatchReader.from(source);
            const iterator = readableDOMStreamToAsyncIterator(reader.asReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('asReadableNodeStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = fs.createReadStream(simpleFilePath);
            const reader = await RecordBatchReader.from(source);
            const iterator = reader.asReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    async function testSimpleAsyncRecordBatchFileReader(reader: RecordBatchReader) {
        reader = await reader.open();
        expect(reader.isFile()).toBe(true);
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
