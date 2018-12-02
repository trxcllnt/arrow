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
import { readableDOMStreamToAsyncIterator } from './util';

import { Schema } from '../../../src/schema';
import { RecordBatch } from '../../../src/recordbatch';
import {
    ArrowDataSource,
    RecordBatchJSONReader,
}  from '../../../src/ipc/reader';

/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

const simpleJSONPath = Path.resolve(__dirname, `../../data/json/simple.json`);
const simpleJSONData = bignumJSONParse('' + fs.readFileSync(simpleJSONPath));

describe('RecordBatchJSONReader', () => {

    it('should read all messages from Arrow JSON data', () => {
        testSimpleRecordBatchJSONReader(new ArrowDataSource(simpleJSONData));
    });

    describe('asReadableDOMStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = new ArrowDataSource(simpleJSONData);
            const reader = source.open() as RecordBatchJSONReader;
            const iterator = readableDOMStreamToAsyncIterator(reader.asReadableDOMStream());
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('asReadableNodeStream', () => {
        it('should yield all RecordBatches', async () => {
            const source = new ArrowDataSource(simpleJSONData);
            const reader = source.open() as RecordBatchJSONReader;
            const iterator = reader.asReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    function testSimpleRecordBatchJSONReader(source: ArrowDataSource) {

        let r: IteratorResult<RecordBatch>;

        const reader = source.open() as RecordBatchJSONReader;

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

        reader.return();
    }
});

async function testSimpleAsyncRecordBatchIterator(iterator: AsyncIterator<RecordBatch>, close = true) {

    let r: IteratorResult<RecordBatch>;

    r = await iterator.next();
    expect(r.done).toBe(false);
    expect(r.value).toBeInstanceOf(RecordBatch);

    r = await iterator.next();
    expect(r.done).toBe(false);
    expect(r.value).toBeInstanceOf(RecordBatch);

    r = await iterator.next();
    expect(r.done).toBe(false);
    expect(r.value).toBeInstanceOf(RecordBatch);

    expect((await iterator.next()).done).toBe(true);

    close && (await iterator.return!());
}
