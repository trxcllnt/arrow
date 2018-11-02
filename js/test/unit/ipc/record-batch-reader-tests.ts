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

import { Schema } from '../../../src/schema';
import { RecordBatch } from '../../../src/recordbatch';
import { Message } from '../../../src/ipc/metadata/message';
import { 
    ArrowDataSource,
    // RecordBatchFileReader, AsyncRecordBatchFileReader,
    RecordBatchStreamReader, AsyncRecordBatchStreamReader,
}  from '../../../src/ipc/reader';

const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
const simpleStreamData = fs.readFileSync(simpleStreamPath);

describe('ArrowDataSource#open', () => {
    it('should return a RecordBatchStreamReader when created with a Uint8Array', () => {
        const source = new ArrowDataSource(simpleStreamData);
        expect(source.isSync()).toEqual(true);
        expect(source.isAsync()).toEqual(false);
        const reader = source.open();
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it('should return a RecordBatchStreamReader when created with an Iterable', () => {
        const source = new ArrowDataSource(function* () { yield simpleStreamData; }());
        expect(source.isSync()).toEqual(true);
        expect(source.isAsync()).toEqual(false);
        const reader = source.open();
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it('should return a RecordBatchStreamReader when created with a Promise<Uint8Array> and should resolve asynchronously', async () => {
        const source = new ArrowDataSource(Promise.resolve(simpleStreamData));
        expect(source.isSync()).toEqual(false);
        expect(source.isAsync()).toEqual(true);
        const pending = source.open();
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it('should return an AsyncRecordBatchStreamReader when created with an AsyncIterable and should resolve asynchronously', async () => {
        const source = new ArrowDataSource(async function* () { yield simpleStreamData; }());
        expect(source.isSync()).toEqual(false);
        expect(source.isAsync()).toEqual(true);
        const pending = source.open();
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader).toBeInstanceOf(AsyncRecordBatchStreamReader);
    });
});

describe('RecordBatchStreamReader', () => {
    it('should read all messages from an Arrow Buffer stream', () => {
        const source = new ArrowDataSource(fs.readFileSync(simpleStreamPath));
        const reader = source.open() as RecordBatchStreamReader;
        simpleRecordBatchStreamReaderTest(reader);
    });

    function simpleRecordBatchStreamReaderTest(reader: RecordBatchStreamReader) {

        let r: IteratorResult<Message>;

        expect(reader.schema).toBeInstanceOf(Schema);

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
        const source = new ArrowDataSource(fs.createReadStream(simpleStreamPath));
        const reader = await source.open() as AsyncRecordBatchStreamReader;
        await simpleAsyncRecordBatchStreamReaderTest(reader);
    });

    // 
    // We can't translate node to DOM streams due to the ReadableStream reference implementation
    // redefining the `byteLength` property of the entire ArrayBuffer to be 0. Node allocates
    // ArrayBuffers in 64k chunks and shares them between active streams, so the current
    // behavior causes just about every test to fail. See the code here for more details:
    // https://github.com/whatwg/streams/blob/3197c7e69456eda08377c18c78ffc99831b5a35f/reference-implementation/lib/helpers.js#L126
    // 
    // it('should read all messages from a whatwg ReadableStream', async () => {
    //     const source = new ArrowDataSource(nodeToWebStream(fs.createReadStream(simpleStreamPath), { type: 'bytes' }));
    //     const reader = await source.open() as AsyncRecordBatchStreamReader;
    //     await simpleAsyncRecordBatchStreamReaderTest(reader);
    // });

    async function simpleAsyncRecordBatchStreamReaderTest(reader: AsyncRecordBatchStreamReader) {

        let r: IteratorResult<Message>;

        expect(reader.schema).toBeInstanceOf(Schema);

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
