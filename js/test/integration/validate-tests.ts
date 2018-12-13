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

import { jsonAndArrowPaths } from './test-config';

if (!jsonAndArrowPaths.length) {
    throw new Error('Integration tests need paths to both json and arrow files');
}

import '../jest-extensions';

import * as path from 'path';
import { zip } from 'ix/iterable/zip';
import {
    Table,
    RecordBatchReader
} from '../Arrow';

describe(`Integration`, () => {
    for (const [json, file] of jsonAndArrowPaths) {

        let { name, dir } = path.parse(file.path);
        dir = dir.split(path.sep).slice(-2).join(path.sep);

        const jsonData = json.data;
        const fileData = file.data;

        describe(path.join(dir, name), () => {
            testReaderIntegration(jsonData, fileData);
            testTableFromBuffersIntegration(jsonData, fileData);
            testTableToBuffersIntegration('json', 'file')(jsonData, fileData);
            testTableToBuffersIntegration('json', 'file')(jsonData, fileData);
            testTableToBuffersIntegration('binary', 'file')(jsonData, fileData);
            testTableToBuffersIntegration('binary', 'file')(jsonData, fileData);
        });
    }
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
        test(`serialized ${srcFormat} ${arrowFormat} reports the same values as the ${refFormat} ${arrowFormat}`, () => {
            expect.hasAssertions();
            const refTable = Table.from(refFormat === `json` ? jsonData : arrowBuffer);
            const srcTable = Table.from(srcFormat === `json` ? jsonData : arrowBuffer);
            const dstTable = Table.from(srcTable.serialize(`binary`, arrowFormat === `stream`));
            expect(dstTable).toEqualTable(refTable);
        });
    }
}
