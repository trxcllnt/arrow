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
    nodeToDOMStream,
    convertNodeToDOMStream,
    readableDOMStreamToAsyncIterator
} from '../util';

import { Schema, RecordBatchReader } from '../../../Arrow';
import {
    testSimpleAsyncRecordBatchIterator,
    testSimpleAsyncRecordBatchStreamReader
} from '../validate';

(() => {

    if (process.env.TEST_DOM_STREAMS !== 'true') { return; }

    /* tslint:disable */
    require('../../../../src/Arrow.dom');

    /* tslint:disable */
    const { concatStream } = require('web-stream-tools').default;

    /* tslint:disable */
    const { parse: bignumJSONParse } = require('json-bignum');

    const simpleJSONPath = Path.resolve(__dirname, `../../../data/json/simple.json`);
    const simpleFilePath = Path.resolve(__dirname, `../../../data/cpp/file/simple.arrow`);
    const simpleStreamPath = Path.resolve(__dirname, `../../../data/cpp/stream/simple.arrow`);
    const simpleFileData = fs.readFileSync(simpleFilePath) as Uint8Array;
    const simpleStreamData = fs.readFileSync(simpleStreamPath) as Uint8Array;
    const simpleJSONData = bignumJSONParse('' + fs.readFileSync(simpleJSONPath)) as { schema: any };

    describe(`RecordBatchReader.throughDOM`, () => {
        it('should read all Arrow file format messages from a whatwg ReadableStream', async () => {
            const stream = fs
                .createReadStream(simpleFilePath)
                .pipe(convertNodeToDOMStream()).dom
                .pipeThrough(RecordBatchReader.throughDOM());
            await testSimpleAsyncRecordBatchIterator(readableDOMStreamToAsyncIterator(stream));
        });
        it('should read all Arrow stream format messages from a whatwg ReadableStream', async () => {
            const stream = fs
                .createReadStream(simpleFilePath)
                .pipe(convertNodeToDOMStream()).dom
                .pipeThrough(RecordBatchReader.throughDOM());
            await testSimpleAsyncRecordBatchIterator(readableDOMStreamToAsyncIterator(stream));
        });
    });

    describe('RecordBatchJSONReader', () => {
        it('toReadableDOMStream should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.from(simpleJSONData);
            const iterator = readableDOMStreamToAsyncIterator(reader.toReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('RecordBatchFileReader', () => {
        it('toReadableDOMStream should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.from(simpleFileData).open();
            const iterator = readableDOMStreamToAsyncIterator(reader.toReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('RecordBatchStreamReader', () => {
        it('toReadableDOMStream should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.from(simpleStreamData);
            const iterator = readableDOMStreamToAsyncIterator(reader.toReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('AsyncRecordBatchFileReader', () => {
        it('toReadableDOMStream should read all RecordBatches from an fs.ReadStream', async () => {
            const reader = await RecordBatchReader.from(fs.createReadStream(simpleFilePath));
            const iterator = readableDOMStreamToAsyncIterator(reader.toReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
        it('toReadableDOMStream should read all RecordBatches from an fs.promises.FileHandle', async () => {
            const reader = await RecordBatchReader.from(await fs.promises.open(simpleFilePath, 'r'));
            const iterator = readableDOMStreamToAsyncIterator(reader.toReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('AsyncRecordBatchStreamReader', () => {
        it('toReadableDOMStream should read all RecordBatches from an fs.ReadStream', async () => {
            const reader = await RecordBatchReader.from(fs.createReadStream(simpleStreamPath));
            const iterator = readableDOMStreamToAsyncIterator(reader.toReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
        it('toReadableDOMStream should read all RecordBatches from an fs.promises.FileHandle', async () => {
            const reader = await RecordBatchReader.from(await fs.promises.open(simpleStreamPath, 'r'));
            const iterator = readableDOMStreamToAsyncIterator(reader.toReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
        it('should read all RecordBatches from a NodeJS ReadableStream that yields multiple tables', async () => {

            const source = concatStream([
                nodeToDOMStream(fs.createReadStream(simpleStreamPath)),
                nodeToDOMStream(fs.createReadStream(simpleStreamPath))
            ]) as ReadableStream<Uint8Array>;
    
            let reader = await (await RecordBatchReader.from(source)).open(false);

            reader = await testSimpleAsyncRecordBatchStreamReader(reader);
            expect(reader.schema).toBeInstanceOf(Schema);
            expect(reader.reset().schema).toBeUndefined();
    
            reader = await testSimpleAsyncRecordBatchStreamReader(reader);
            await reader.cancel();
            expect(reader.schema).toBeUndefined();
            expect((await reader.open()).schema).toBeUndefined();
        });

        // /* tslint:disable */
        // const { concatStream } = require('web-stream-tools').default;
    
        // it('should read multiple tables from the same DOM stream', async () => {
    
        //     const sources = concatStream(jsonAndArrowPaths.map(([, , stream]) => {
        //         return nodeToDOMStream(fs.createReadStream(stream.path));
        //     })) as ReadableStream<Uint8Array>;
    
        //     let reader = await RecordBatchReader.from(sources);
    
        //     for (const [json, file] of jsonAndArrowPaths) {
    
        //         reader = await reader.reset().open(false);
    
        //         const jsonTable = Table.from(json.data);
        //         const fileTable = Table.from(file.data);
        //         const streamTable = await Table.from(reader);
    
        //         expect(streamTable).toEqualTable(jsonTable);
        //         expect(streamTable).toEqualTable(fileTable);
        //     }
    
        //     reader.cancel();
        // }, 60 * 1000);
    });
})();
