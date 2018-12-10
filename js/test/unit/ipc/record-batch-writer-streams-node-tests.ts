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
import { RecordBatchReader, RecordBatchWriter } from '../../Arrow';
import {
    testSimpleAsyncRecordBatchIterator
} from './validate';

(() => {

    if (process.env.TEST_NODE_STREAMS !== 'true') { return; }

    /* tslint:disable */
    require('../../../src/Arrow.node');
    /* tslint:disable */
    // const concatStream = require('multistream');
    /* tslint:disable */
    // const { parse: bignumJSONParse } = require('json-bignum');

    const simpleFilePath = Path.resolve(__dirname, `../../data/cpp/file/simple.arrow`);
    const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);

    describe(`RecordBatchWriter.throughNode`, () => {
        it('should read all Arrow file format messages from an fs.ReadStream', async () => {

            const stream = fs
                .createReadStream(simpleFilePath)
                .pipe(RecordBatchReader.throughNode())
                .pipe(RecordBatchWriter.throughNode())
                .pipe(RecordBatchReader.throughNode());

            await testSimpleAsyncRecordBatchIterator(stream[Symbol.asyncIterator]());
        });
        it('should read all Arrow stream format messages from an fs.ReadStream', async () => {

            const stream = fs
                .createReadStream(simpleStreamPath)
                .pipe(RecordBatchReader.throughNode())
                .pipe(RecordBatchWriter.throughNode())
                .pipe(RecordBatchReader.throughNode());

            await testSimpleAsyncRecordBatchIterator(stream[Symbol.asyncIterator]());
        });
    });

    // describe('RecordBatchJSONReader', () => {
    //     it('asReadableNodeStream should yield all RecordBatches', async () => {
    //         const reader = RecordBatchReader.from(simpleJSONData);
    //         const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
    //         await testSimpleAsyncRecordBatchIterator(iterator);
    //     });
    // });

    // describe('RecordBatchFileReader', () => {
    //     it('asReadableNodeStream should yield all RecordBatches', async () => {
    //         const reader = RecordBatchReader.from(simpleFileData).open();
    //         const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
    //         await testSimpleAsyncRecordBatchIterator(iterator);
    //     });
    // });

    // describe('RecordBatchStreamReader', () => {
    //     it('asReadableNodeStream should yield all RecordBatches', async () => {
    //         const reader = RecordBatchReader.from(simpleStreamData);
    //         const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
    //         await testSimpleAsyncRecordBatchIterator(iterator);
    //     });
    // });

    // describe('AsyncRecordBatchFileReader', () => {
    //     it('asReadableNodeStream should read all RecordBatches from an fs.ReadStream', async () => {
    //         const reader = await RecordBatchReader.from(fs.createReadStream(simpleFilePath));
    //         const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
    //         await testSimpleAsyncRecordBatchIterator(iterator);
    //     });
    //     it('asReadableNodeStream should read all RecordBatches from an fs.promises.FileHandle', async () => {
    //         const reader = await RecordBatchReader.from(await fs.promises.open(simpleFilePath, 'r'));
    //         const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
    //         await testSimpleAsyncRecordBatchIterator(iterator);
    //     });
    // });

    // describe('AsyncRecordBatchStreamReader', () => {
    //     it('asReadableNodeStream should read all RecordBatches from an fs.ReadStream', async () => {
    //         const reader = await RecordBatchReader.from(fs.createReadStream(simpleStreamPath));
    //         const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
    //         await testSimpleAsyncRecordBatchIterator(iterator);
    //     });
    //     it('asReadableNodeStream should read all RecordBatches from an fs.promises.FileHandle', async () => {
    //         const reader = await RecordBatchReader.from(await fs.promises.open(simpleStreamPath, 'r'));
    //         const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
    //         await testSimpleAsyncRecordBatchIterator(iterator);
    //     });
    //     it('should read all RecordBatches from a NodeJS ReadableStream that yields multiple tables', async () => {

    //         const source = concatStream([
    //             () => fs.createReadStream(simpleStreamPath),
    //             () => fs.createReadStream(simpleStreamPath)
    //         ]);
    
    //         let reader = await (await RecordBatchReader.from(source)).open(false);

    //         reader = await testSimpleAsyncRecordBatchStreamReader(reader);
    //         expect(reader.schema).toBeInstanceOf(Schema);
    //         expect(reader.reset().schema).toBeUndefined();
    
    //         reader = await testSimpleAsyncRecordBatchStreamReader(reader);
    //         expect((await reader.close()).schema).toBeUndefined();
    //         expect((await reader.open()).schema).toBeUndefined();
    //     });
    // });
})();
