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
import * as generate from '../../../test-data';

import {
    Table,
    Schema, Field,
    RecordBatchReader,
    RecordBatchStreamWriter
} from '../../../Arrow';

import {
    testSimpleAsyncRecordBatchIterator,
    testSimpleAsyncRecordBatchStreamReader
} from '../validate';

(() => {

    if (process.env.TEST_NODE_STREAMS !== 'true') {
        return test('not testing node streams because process.env.TEST_NODE_STREAMS !== "true"', () => {});
    }

    /* tslint:disable */
    const stream = require('stream');
    /* tslint:disable */
    const concatStream = ((multistream) => (...xs: any[]) =>
        new stream.Readable().wrap(multistream(...xs))
    )(require('multistream'));
    /* tslint:disable */
    const { parse: bignumJSONParse } = require('json-bignum');

    const simpleJSONPath = Path.resolve(__dirname, `../../../data/json/simple.json`);
    const simpleFilePath = Path.resolve(__dirname, `../../../data/cpp/file/simple.arrow`);
    const simpleStreamPath = Path.resolve(__dirname, `../../../data/cpp/stream/simple.arrow`);
    const simpleFileData = fs.readFileSync(simpleFilePath) as Uint8Array;
    const simpleStreamData = fs.readFileSync(simpleStreamPath) as Uint8Array;
    const simpleJSONData = bignumJSONParse('' + fs.readFileSync(simpleJSONPath)) as { schema: any };

    describe(`RecordBatchReader.throughNode`, () => {
        it('should read all Arrow file format messages from an fs.ReadStream', async () => {
            const stream = fs
                .createReadStream(simpleFilePath)
                .pipe(RecordBatchReader.throughNode());
            await testSimpleAsyncRecordBatchIterator(stream[Symbol.asyncIterator]());
        });
        it('should read all Arrow stream format messages from an fs.ReadStream', async () => {
            const stream = fs
                .createReadStream(simpleStreamPath)
                .pipe(RecordBatchReader.throughNode());
            await testSimpleAsyncRecordBatchIterator(stream[Symbol.asyncIterator]());
        });
    });

    describe('RecordBatchJSONReader', () => {
        it('toReadableNodeStream should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.from(simpleJSONData);
            const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('RecordBatchFileReader', () => {
        it('toReadableNodeStream should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.from(simpleFileData).open();
            const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('RecordBatchStreamReader', () => {
        it('toReadableNodeStream should yield all RecordBatches', async () => {
            const reader = RecordBatchReader.from(simpleStreamData);
            const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('AsyncRecordBatchFileReader', () => {
        it('toReadableNodeStream should read all RecordBatches from an fs.ReadStream', async () => {
            const reader = await RecordBatchReader.from(fs.createReadStream(simpleFilePath));
            const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
        it('toReadableNodeStream should read all RecordBatches from an fs.promises.FileHandle', async () => {
            const reader = await RecordBatchReader.from(await fs.promises.open(simpleFilePath, 'r'));
            const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
    });

    describe('AsyncRecordBatchStreamReader', () => {
        it('toReadableNodeStream should read all RecordBatches from an fs.ReadStream', async () => {
            const reader = await RecordBatchReader.from(fs.createReadStream(simpleStreamPath));
            const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
        it('toReadableNodeStream should read all RecordBatches from an fs.promises.FileHandle', async () => {
            const reader = await RecordBatchReader.from(await fs.promises.open(simpleStreamPath, 'r'));
            const iterator = reader.toReadableNodeStream()[Symbol.asyncIterator]();
            await testSimpleAsyncRecordBatchIterator(iterator);
        });
        it('should read all RecordBatches from a NodeJS ReadableStream that yields multiple tables', async () => {

            const source = concatStream([
                () => fs.createReadStream(simpleStreamPath),
                () => fs.createReadStream(simpleStreamPath)
            ]) as NodeJS.ReadableStream;
    
            let reader = await (await RecordBatchReader.from(source)).open(false);

            reader = await testSimpleAsyncRecordBatchStreamReader(reader);
            expect(reader.schema).toBeInstanceOf(Schema);
            expect(reader.reset().schema).toBeUndefined();
    
            reader = await testSimpleAsyncRecordBatchStreamReader(reader);
            await reader.cancel();
            expect(reader.schema).toBeUndefined();
            expect((await reader.open()).schema).toBeUndefined();
        });

        it('should not close the underlying NodeJS ReadableStream when reading multiple tables', async () => {

            expect.hasAssertions();

            const tables = [
                ['float64', 'dateDay', 'null_', 'timestampMicrosecond'],
                ['sparseUnion', 'uint8', 'int16', 'timeMillisecond', 'float32', 'int8'],
                ['intervalYearMonth', 'timeSecond', 'uint32'],
                ['timeMicrosecond'],
                ['uint64', 'timestampNanosecond', 'map', 'bool'],
                ['denseUnion', 'fixedSizeBinary', 'struct', 'list'],
                ['int32', 'intervalDayTime', 'fixedSizeList', 'uint16'],
                ['dictionary', 'int64', 'utf8', 'timeNanosecond', 'timestampSecond'],
                ['dateMillisecond', 'float16', 'binary', 'decimal', 'timestampMillisecond']
            ].map((fns) => {
                const types = fns.map((fn) => (generate as any)[fn](0).type);
                types.forEach((t) => t.dictionaryVector && (t.dictionaryVector = null));
                const schema = new Schema(fns.map((name, i) => new Field(name, types[i])));
                return generate.table([
                    Math.random() * 100 | 0,
                    Math.random() * 200 | 0,
                    Math.random() * 300 | 0
                ], schema);
            });

            const stream = concatStream(tables.map((table) =>
                () => RecordBatchStreamWriter.writeAll(table.batches).toReadableNodeStream()
            )) as NodeJS.ReadableStream;
    
            let index = -1;
            let reader = await RecordBatchReader.from(stream);

            try {
                while (!(await reader.reset().open(false)).closed) {
                    const sourceTable = tables[++index];
                    const streamTable = await Table.from(reader);
                    expect(streamTable).toEqualTable(sourceTable);
                }
            } finally { reader.cancel(); }

            expect(index).toBe(tables.length - 1);
        });
    });
})();
