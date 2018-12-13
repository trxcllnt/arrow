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

import {
    RecordBatchReader,
    RecordBatchWriter,
    RecordBatchFileWriter,
    RecordBatchStreamWriter,
} from '../../../Arrow';

import {
    testSimpleRecordBatchFileReader,
    testSimpleRecordBatchStreamReader,
    testSimpleAsyncRecordBatchStreamReader,
} from '../validate';

const simpleFilePath = Path.resolve(__dirname, `../../../data/cpp/file/simple.arrow`);
const simpleStreamPath = Path.resolve(__dirname, `../../../data/cpp/stream/simple.arrow`);
const simpleFileData = fs.readFileSync(simpleFilePath) as Uint8Array;
const simpleStreamData = fs.readFileSync(simpleStreamPath) as Uint8Array;

describe('RecordBatchWriter', () => {
    it('should write RecordBatches to a stream', async () => {
        const writer = new RecordBatchWriter();
        for (const batch of RecordBatchReader.from(simpleStreamData)) {
            writer.write(batch);
        }
        writer.close();
        const reader = await RecordBatchReader.from(writer);
        await testSimpleAsyncRecordBatchStreamReader(reader);
    });
});

describe('RecordBatchFileWriter', () => {
    it('should write the Arrow file format', async () => {
        const writer = new RecordBatchFileWriter();
        for (const batch of RecordBatchReader.from(simpleStreamData)) {
            writer.write(batch);
        }
        writer.close();
        const reader = RecordBatchReader.from(await writer.toUint8Array());
        testSimpleRecordBatchFileReader(reader);
    });
});

describe('RecordBatchStreamWriter', () => {
    it('should write the Arrow stream format', async () => {
        const writer = new RecordBatchStreamWriter();
        for (const batch of RecordBatchReader.from(simpleFileData)) {
            writer.write(batch);
        }
        writer.close();
        const reader = RecordBatchReader.from(await writer.toUint8Array());
        testSimpleRecordBatchStreamReader(reader);
    });
});
