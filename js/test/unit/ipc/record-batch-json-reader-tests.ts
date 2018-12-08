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
import { readableDOMStreamToAsyncIterator } from './util';

import { Schema } from '../../../src/schema';
import { RecordBatch } from '../../../src/recordbatch';
import { ArrowJSONLike } from '../../../src/io/interfaces';
import { RecordBatchReader }  from '../../../src/ipc/reader';
import { RecordBatchStreamReader } from '../../../src/ipc/reader';

/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

const simpleJSONPath = Path.resolve(__dirname, `../../data/json/simple.json`);
const simpleJSONData = bignumJSONParse('' + fs.readFileSync(simpleJSONPath)) as ArrowJSONLike;

describe('RecordBatchJSONReader', () => {

    it('should read all messages from Arrow JSON data', () => {
        testSimpleRecordBatchJSONReader(RecordBatchReader.from(simpleJSONData));
    });

    describe('asReadableDOMStream', () => {
        it('should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.from(simpleJSONData);
            const iterator = readableDOMStreamToAsyncIterator(reader.asReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('asReadableNodeStream', () => {
        it('should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.from(simpleJSONData);
            const iterator = reader.asReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    function testSimpleRecordBatchJSONReader(reader: RecordBatchStreamReader) {

        reader = reader.open();
        expect(reader.isStream()).toBe(true);
        expect(reader.schema).toBeInstanceOf(Schema);

        const batches = [...reader];

        expect(batches.length).toEqual(3);

        batches.forEach((b, i) => {
            try {
                expect(b).toBeInstanceOf(RecordBatch)
            } catch (e) { throw new Error(`${i}: ${e}`); }
        });

        return reader;
    }
});

async function testSimpleAsyncRecordBatchIterator(iterator: AsyncIterableIterator<RecordBatch>) {
    const batches = await toArray(iterator);
    expect(batches.length).toEqual(3);
    batches.forEach((b, i) => {
        try {
            expect(b).toBeInstanceOf(RecordBatch)
        } catch (e) { throw new Error(`${i}: ${e}`); }
    });
    await iterator.return!();
}
