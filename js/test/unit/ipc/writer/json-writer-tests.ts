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

import {
    generateRandomTables,
    generateDictionaryTables
} from '../../../data/tables';

import { Table, RecordBatchJSONWriter } from '../../../Arrow';

/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

describe('RecordBatchJSONWriter', () => {

    for (const table of generateRandomTables([20, 30, 40, 50])) {
        let name = `[${table.schema.fields.join(', ')}]`;
        it(`should write the Arrow IPC JSON format (${name})`, () => {
            try {
                const writer = RecordBatchJSONWriter.writeAll(table);
                const data = `${Buffer.from(writer.toUint8Array(true))}`;
                const jsonTable = Table.from(bignumJSONParse(data));
                expect(jsonTable).toEqualTable(table);
            } catch (e) { throw new Error(`${name}: ${e}`); }
        });
    }

    for (const table of generateDictionaryTables([100, 200, 300])) {
        let name = `${table.schema.fields[0]}`;
        it(`should write dictionary batches in IPC JSON format (${name})`, () => {
            try {
                const writer = RecordBatchJSONWriter.writeAll(table);
                const data = `${Buffer.from(writer.toUint8Array(true))}`;
                const jsonTable = Table.from(bignumJSONParse(data));
                expect(jsonTable).toEqualTable(table);
            } catch (e) { throw new Error(`${name}: ${e}`); }
        });
    }
});
