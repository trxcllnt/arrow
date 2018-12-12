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
    concatBuffersAsync,
    convertNodeToDOMStream,
    readableDOMStreamToAsyncIterator
} from './util';
import {
    RecordBatchReader,
    RecordBatchWriter,
    RecordBatchFileWriter,
    RecordBatchStreamWriter
} from '../../Arrow';
import {
    testSimpleRecordBatchFileReader,
    testSimpleRecordBatchStreamReader,
    testSimpleAsyncRecordBatchIterator,
    testSimpleAsyncRecordBatchFileReader,
    testSimpleAsyncRecordBatchStreamReader
} from './validate';

(() => {

    if (process.env.TEST_NODE_STREAMS !== 'true') { return; }

    /* tslint:disable */
    require('../../../src/Arrow.node');
    /* tslint:disable */
    // const concatStream = require('multistream');
    /* tslint:disable */
    // const { parse: bignumJSONParse } = require('json-bignum');

    // const simpleJSONPath = Path.resolve(__dirname, `../../data/json/simple.json`);
    const simpleFilePath = Path.resolve(__dirname, `../../data/cpp/file/simple.arrow`);
    const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
    const simpleFileData = fs.readFileSync(simpleFilePath) as Uint8Array;
    const simpleStreamData = fs.readFileSync(simpleStreamPath) as Uint8Array;
    // const simpleJSONData = bignumJSONParse('' + fs.readFileSync(simpleJSONPath)) as { schema: any };

    describe(`RecordBatchWriter.throughNode`, () => {
        it('should read all Arrow file format messages from an fs.ReadStream', async () => {

            const stream = fs
                .createReadStream(simpleFilePath)
                .pipe(convertNodeToDOMStream()).dom
                .pipeThrough(RecordBatchReader.throughDOM())
                .pipeThrough(RecordBatchWriter.throughDOM())
                .pipeThrough(RecordBatchReader.throughDOM());

            await testSimpleAsyncRecordBatchIterator(readableDOMStreamToAsyncIterator(stream));
        });
        it('should read all Arrow stream format messages from an fs.ReadStream', async () => {

            const stream = fs
                .createReadStream(simpleStreamPath)
                .pipe(convertNodeToDOMStream()).dom
                .pipeThrough(RecordBatchReader.throughDOM())
                .pipeThrough(RecordBatchWriter.throughDOM())
                .pipeThrough(RecordBatchReader.throughDOM());

            await testSimpleAsyncRecordBatchIterator(readableDOMStreamToAsyncIterator(stream));
        });
    });

    describe(`RecordBatchFileWriter.throughNode`, () => {
        it('should convert an Arrow stream to file format', async () => {

            const stream = fs
                .createReadStream(simpleStreamPath)
                .pipe(convertNodeToDOMStream()).dom
                .pipeThrough(RecordBatchReader.throughDOM())
                .pipeThrough(RecordBatchFileWriter.throughDOM());

            const buffer = await concatBuffersAsync(readableDOMStreamToAsyncIterator(stream));

            testSimpleRecordBatchFileReader(RecordBatchReader.from(buffer));
        });
        it('should convert an Arrow stream to file format (async)', async () => {

            const stream = fs
                .createReadStream(simpleStreamPath)
                .pipe(convertNodeToDOMStream()).dom
                .pipeThrough(RecordBatchReader.throughDOM())
                .pipeThrough(RecordBatchFileWriter.throughDOM());

            const reader = await RecordBatchReader.from(readableDOMStreamToAsyncIterator(stream));

            testSimpleAsyncRecordBatchFileReader(reader);
        });
    });

    describe(`RecordBatchStreamWriter.throughNode`, () => {
        it('should convert an Arrow file to stream format', async () => {

            const stream = fs
                .createReadStream(simpleFilePath)
                .pipe(convertNodeToDOMStream()).dom
                .pipeThrough(RecordBatchReader.throughDOM())
                .pipeThrough(RecordBatchStreamWriter.throughDOM());

            const buffer = await concatBuffersAsync(readableDOMStreamToAsyncIterator(stream));

            testSimpleRecordBatchStreamReader(RecordBatchReader.from(buffer));
        });
        it('should convert an Arrow file to stream format (async)', async () => {

            const stream = fs
                .createReadStream(simpleFilePath)
                .pipe(convertNodeToDOMStream()).dom
                .pipeThrough(RecordBatchReader.throughDOM())
                .pipeThrough(RecordBatchStreamWriter.throughDOM());

            const reader = await RecordBatchReader.from(stream);

            testSimpleAsyncRecordBatchStreamReader(reader);
        });
    });

    describe('RecordBatchFileWriter', () => {
        it('toReadableDOMStream should return an Arrow file ReadableStream', async () => {
            const writer = new RecordBatchFileWriter();
            for (const batch of RecordBatchReader.from(simpleStreamData)) {
                writer.write(batch);
            }
            writer.close();
            const reader = await RecordBatchReader.from(writer.toReadableDOMStream());
            testSimpleAsyncRecordBatchFileReader(reader);
        });
    });
    
    describe('RecordBatchStreamWriter', () => {
        it('toReadableDOMStream should return an Arrow stream ReadableStream', async () => {
            const writer = new RecordBatchStreamWriter();
            for (const batch of RecordBatchReader.from(simpleFileData)) {
                writer.write(batch);
            }
            writer.close();
            const reader = await RecordBatchReader.from(writer.toReadableDOMStream());
            testSimpleAsyncRecordBatchStreamReader(reader);
        });
    });
})();
