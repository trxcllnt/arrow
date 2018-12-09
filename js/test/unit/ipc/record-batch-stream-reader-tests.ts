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

import * as fs from 'fs';
import * as Path from 'path';
import { Schema, RecordBatchReader } from '../../Arrow';
import { nodeToDOMStream, chunkedIterable, asyncChunkedIterable } from './util';
import {
    testSimpleRecordBatchStreamReader,
    testSimpleAsyncRecordBatchStreamReader,
} from './validate';

const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
const simpleStreamData = fs.readFileSync(simpleStreamPath);

describe('RecordBatchStreamReader', () => {
    it('should read all RecordBatches from a Buffer of Arrow messages in memory', () => {
        testSimpleRecordBatchStreamReader(RecordBatchReader.from(simpleStreamData));
    });
    it('should read all RecordBatches from an Iterable that yields buffers of Arrow messages in memory', () => {
        testSimpleRecordBatchStreamReader(RecordBatchReader.from(chunkedIterable(simpleStreamData)));
    });
    it('should read all RecordBatches from a Promise<Buffer> of Arrow messages in memory', async () => {
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from((async () => simpleStreamData)()));
    });
    it('should read all RecordBatches from an Iterable that yields multiple tables as buffers of Arrow messages in memory', () => {

        const source = (function *() {
            yield* chunkedIterable(simpleStreamData);
            yield* chunkedIterable(simpleStreamData);
        }());

        let reader = RecordBatchReader.from(source);

        reader = testSimpleRecordBatchStreamReader(reader.open(false));
        expect(reader.schema).toBeInstanceOf(Schema);
        expect(reader.reset().schema).toBeUndefined();

        reader = testSimpleRecordBatchStreamReader(reader.open(false));
        expect(reader.close().schema).toBeUndefined();
        expect(reader.open().schema).toBeUndefined();
    });
});

describe('AsyncRecordBatchStreamReader', () => {

    it('should read all RecordBatches from an AsyncIterable that yields buffers of Arrow messages in memory', async () => {
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from(asyncChunkedIterable(simpleStreamData)));
    });

    it('should read all RecordBatches from an fs.ReadStream', async () => {
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from(fs.createReadStream(simpleStreamPath)));
    });

    it('should read all RecordBatches from an fs.promises.FileHandle', async () => {
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from(await fs.promises.open(simpleStreamPath, 'r')));
    });

    it('should read all RecordBatches from a whatwg ReadableStream', async () => {
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from(nodeToDOMStream(fs.createReadStream(simpleStreamPath))));
    });

    it('should read all RecordBatches from a whatwg ReadableByteStream', async () => {
        await testSimpleAsyncRecordBatchStreamReader(await RecordBatchReader.from(nodeToDOMStream(fs.createReadStream(simpleStreamPath), { type: 'bytes' })));
    });

    it('should read all RecordBatches from an AsyncIterable that yields multiple tables as buffers of Arrow messages in memory', async () => {

        const source = (async function *() {
            yield* asyncChunkedIterable(simpleStreamData);
            yield* asyncChunkedIterable(simpleStreamData);
        }());

        let reader = await RecordBatchReader.from(source);

        reader = await testSimpleAsyncRecordBatchStreamReader(await reader.open(false));
        expect(reader.schema).toBeInstanceOf(Schema);
        expect(reader.reset().schema).toBeUndefined();

        reader = await testSimpleAsyncRecordBatchStreamReader(await reader.open(false));
        expect((await reader.close()).schema).toBeUndefined();
        expect((await reader.open()).schema).toBeUndefined();
    });
});
