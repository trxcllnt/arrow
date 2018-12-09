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
import { toArray } from 'ix/asynciterable/toarray';
import { chunkedIterable, asyncChunkedIterable } from './util';
import {
    testSimpleRecordBatchFileReader,
    testSimpleAsyncRecordBatchFileReader,
} from './validate';

import {
    RecordBatchReader,
    RecordBatchFileReader,
    AsyncRecordBatchFileReader 
} from '../../Arrow';

const simpleFilePath = Path.resolve(__dirname, `../../data/cpp/file/simple.arrow`);
const simpleFileData = fs.readFileSync(simpleFilePath);

describe('RecordBatchFileReader', () => {
    it('should read all RecordBatches from a Buffer of Arrow data in memory', () => {
        testSimpleRecordBatchFileReader(RecordBatchReader.from(simpleFileData));
    });
    it('should read all RecordBatches from an Iterable that yields buffers of Arrow messages in memory', () => {
        testSimpleRecordBatchFileReader(RecordBatchReader.from(chunkedIterable(simpleFileData)));
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
});

describe('AsyncRecordBatchFileReader', () => {

    it('should read all RecordBatches from an fs.ReadStream', async () => {
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(fs.createReadStream(simpleFilePath)));
    });

    it('should read all RecordBatches from a Promise<fs.promises.FileHandle>', async () => {
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(fs.promises.open(simpleFilePath, 'r')));
    });

    it('should read all RecordBatches from an fs.promises.FileHandle', async () => {
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(await fs.promises.open(simpleFilePath, 'r')));
    });

    it('should read all RecordBatches from an AsyncIterable that yields buffers of Arrow messages in memory', async () => {
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(asyncChunkedIterable(simpleFileData)));
    });

    it('should read all RecordBatches from a whatwg ReadableStream', async () => {
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(nodeToDOMStream(fs.createReadStream(simpleFilePath))));
    });

    it('should read all RecordBatches from a whatwg ReadableByteStream', async () => {
        await testSimpleAsyncRecordBatchFileReader(await RecordBatchReader.from(nodeToDOMStream(fs.createReadStream(simpleFilePath), { type: 'bytes' })));
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
});
