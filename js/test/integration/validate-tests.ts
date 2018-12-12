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

if (!process.env.JSON_PATHS || !process.env.ARROW_PATHS) {
    throw new Error('Integration tests need paths to both json and arrow files');
}

import '../jest-extensions';

import * as fs from 'fs';
import * as path from 'path';

import { zip } from 'ix/iterable/zip';
import { toArray } from 'ix/iterable/toarray';
import { Table, RecordBatchReader } from '../Arrow';

/* tslint:disable */
const concatStream = require('multistream');
/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

function resolvePathArgs(paths: string) {
    let pathsArray = JSON.parse(paths) as string | string[];
    return (Array.isArray(pathsArray) ? pathsArray : [pathsArray])
        .map((p) => path.resolve(p))
        .map((p) => {
            if (fs.existsSync(p)) {
                return p;
            }
            console.error(`Could not find file "${p}"`);
            return undefined;
        });
}

const getOrReadFileBuffer = ((cache: any) => function getFileBuffer(path: string, ...args: any[]) {
    return cache[path] || (cache[path] = fs.readFileSync(path, ...args));
})({});

const jsonAndArrowPaths = toArray(zip(
    resolvePathArgs(process.env.JSON_PATHS!),
    resolvePathArgs(process.env.ARROW_PATHS!)
))
.filter(([p1, p2]) => p1 !== undefined && p2 !== undefined) as [string, string][];

describe(`Integration`, () => {
    for (const [jsonFilePath, arrowFilePath] of jsonAndArrowPaths) {
        let { name, dir } = path.parse(arrowFilePath);
        dir = dir.split(path.sep).slice(-2).join(path.sep);
        const json = bignumJSONParse(getOrReadFileBuffer(jsonFilePath, 'utf8'));
        const arrowBuffer = getOrReadFileBuffer(arrowFilePath) as Uint8Array;
        describe(path.join(dir, name), () => {
            testReaderIntegration(json, arrowBuffer);
            testTableFromBuffersIntegration(json, arrowBuffer);
            testTableToBuffersIntegration('json', 'file')(json, arrowBuffer);
            testTableToBuffersIntegration('binary', 'file')(json, arrowBuffer);
            testTableToBuffersIntegration('json', 'stream')(json, arrowBuffer);
            testTableToBuffersIntegration('binary', 'stream')(json, arrowBuffer);
        });
    }
    testReadingMultipleTablesFromTheSameStream();
});

function testReaderIntegration(jsonData: any, arrowBuffer: Uint8Array) {
    test(`json and arrow record batches report the same values`, () => {
        expect.hasAssertions();
        const jsonReader = RecordBatchReader.from(jsonData);
        const binaryReader = RecordBatchReader.from(arrowBuffer);
        for (const [jsonRecordBatch, binaryRecordBatch] of zip(jsonReader, binaryReader)) {
            expect(jsonRecordBatch).toEqualRecordBatch(binaryRecordBatch);
        }
    });
}

function testTableFromBuffersIntegration(jsonData: any, arrowBuffer: Uint8Array) {
    test(`json and arrow tables report the same values`, () => {
        expect.hasAssertions();
        const jsonTable = Table.from(jsonData);
        const binaryTable = Table.from(arrowBuffer);
        expect(jsonTable).toEqualTable(binaryTable);
    });
}

function testTableToBuffersIntegration(srcFormat: 'json' | 'binary', arrowFormat: 'stream' | 'file') {
    const refFormat = srcFormat === `json` ? `binary` : `json`;
    return function testTableToBuffersIntegration(jsonData: any, arrowBuffer: Uint8Array) {
        test(`serialized ${srcFormat} ${arrowFormat} reports the same values as the ${refFormat} ${arrowFormat}`, async () => {
            expect.hasAssertions();
            const refTable = Table.from(refFormat === `json` ? jsonData : arrowBuffer);
            const srcTable = Table.from(srcFormat === `json` ? jsonData : arrowBuffer);
            const dstTable = Table.from(srcTable.serialize(`binary`, arrowFormat === `stream`));
            expect(dstTable).toEqualTable(refTable);
        });
    }
}

function testReadingMultipleTablesFromTheSameStream() {

    test('Can read multiple tables from the same stream', async () => {

        const sources = concatStream([...jsonAndArrowPaths].map(([, arrowPath]) => {
            return () => fs.createReadStream(arrowPath);
        })) as NodeJS.ReadableStream;

        const reader = await RecordBatchReader.from(sources);

        for (const [jsonFilePath, arrowFilePath] of jsonAndArrowPaths) {

            const streamTable = await Table.from(await reader.reset().open(false));
            const binaryTable = Table.from(getOrReadFileBuffer(arrowFilePath) as Uint8Array);
            const jsonTable = Table.from(bignumJSONParse(getOrReadFileBuffer(jsonFilePath, 'utf8')));

            expect(streamTable).toEqualTable(jsonTable);
            expect(streamTable).toEqualTable(binaryTable);
        }

        reader.cancel();
    });
}
