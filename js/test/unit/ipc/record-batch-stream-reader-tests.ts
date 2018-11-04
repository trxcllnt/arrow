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
import { nodeToDOMStream } from './util';

import { Schema } from '../../../src/schema';
import { RecordBatch } from '../../../src/recordbatch';
import { 
    ArrowDataSource,
    RecordBatchStreamReader, AsyncRecordBatchStreamReader,
}  from '../../../src/ipc/reader';

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

    it('should read all messages from an Buffer of Arrow data in memory', () => {
        testSimpleRecordBatchStreamReader(new ArrowDataSource(simpleStreamData));
    });

    it('should read all messages from an Iterable that yields buffers of Arrow messages in memory', () => {
        testSimpleRecordBatchStreamReader(new ArrowDataSource(iterableSource(simpleStreamData)));
    });

    it('should read all messages from an Iterable that yields multiple tables as buffers of Arrow messages in memory', () => {
        const source = new ArrowDataSource(function *() {
            yield* iterableSource(simpleStreamData);
            yield* iterableSource(simpleStreamData);
        }());
        testSimpleRecordBatchStreamReader(source);
        testSimpleRecordBatchStreamReader(source);
    });

    function testSimpleRecordBatchStreamReader(source: ArrowDataSource) {

        let r: IteratorResult<RecordBatch>;

        const reader = source.open() as RecordBatchStreamReader;

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
    }
});

describe('AsyncMessageReader', () => {

    it('should read all messages from a NodeJS ReadableStream', async () => {
        await testSimpleAsyncRecordBatchStreamReader(new ArrowDataSource(fs.createReadStream(simpleStreamPath)));
    });

    it('should read all messages from an AsyncIterable that yields buffers of Arrow messages in memory', async () => {
        await testSimpleAsyncRecordBatchStreamReader(new ArrowDataSource(asyncIterableSource(simpleStreamData)));
    });

    it('should read all messages from an AsyncIterable that yields multiple tables as buffers of Arrow messages in memory', async () => {
        const source = new ArrowDataSource(async function *() {
            yield* asyncIterableSource(simpleStreamData);
            yield* asyncIterableSource(simpleStreamData);
        }());
        await testSimpleAsyncRecordBatchStreamReader(source);
        await testSimpleAsyncRecordBatchStreamReader(source);
    });

    it('should read all messages from a whatwg ReadableStream', async () => {
        await testSimpleAsyncRecordBatchStreamReader(new ArrowDataSource(
            nodeToDOMStream(fs.createReadStream(simpleStreamPath), { type: 'bytes' })));
    });

    async function testSimpleAsyncRecordBatchStreamReader(source: ArrowDataSource) {

        let r: IteratorResult<RecordBatch>;

        let reader = await source.open() as AsyncRecordBatchStreamReader;

        expect(await reader.readSchema()).toBeInstanceOf(Schema);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(RecordBatch);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(RecordBatch);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(RecordBatch);

        expect((await reader.next()).done).toBe(true);
    }
});
