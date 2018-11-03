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

import { 
    ArrowDataSource,
    RecordBatchFileReader, AsyncRecordBatchFileReader,
    RecordBatchStreamReader, AsyncRecordBatchStreamReader,
}  from '../../../src/ipc/reader';

const simpleFilePath = Path.resolve(__dirname, `../../data/cpp/file/simple.arrow`);
const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
const simpleFileData = fs.readFileSync(simpleFilePath);
const simpleStreamData = fs.readFileSync(simpleStreamPath);

describe('ArrowDataSource#open', () => {
    testFileDataSource();
    testStreamDataSource();
});

function testFileDataSource() {
    const buffer = simpleFileData;
    it(`should return a ${RecordBatchFileReader.name} when created with a Uint8Array`, () => {
        const source = new ArrowDataSource(buffer);
        expect(source.isSync()).toEqual(true);
        expect(source.isAsync()).toEqual(false);
        const reader = source.open();
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return a ${RecordBatchFileReader.name} when created with an Iterable`, () => {
        const source = new ArrowDataSource(function* () { yield buffer; }());
        expect(source.isSync()).toEqual(true);
        expect(source.isAsync()).toEqual(false);
        const reader = source.open();
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return a ${RecordBatchFileReader.name} when created with a Promise<Uint8Array> and should resolve asynchronously`, async () => {
        const source = new ArrowDataSource(Promise.resolve(buffer));
        expect(source.isSync()).toEqual(false);
        expect(source.isAsync()).toEqual(true);
        const pending = source.open();
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return a ${RecordBatchFileReader.name} when created with an AsyncIterable and should resolve asynchronously`, async () => {
        const source = new ArrowDataSource(async function* () { yield buffer; }());
        expect(source.isSync()).toEqual(false);
        expect(source.isAsync()).toEqual(true);
        const pending = source.open();
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return an ${AsyncRecordBatchFileReader.name} when created with a FileHandle and should resolve asynchronously`, async () => {
        const source = new ArrowDataSource(await fs.promises.open(simpleFilePath, 'r'));
        expect(source.isSync()).toEqual(false);
        expect(source.isAsync()).toEqual(true);
        const pending = source.open();
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader).toBeInstanceOf(AsyncRecordBatchFileReader);
    });
    it(`should return an ${AsyncRecordBatchFileReader.name} when created with a Promise<FileHandle> and should resolve synchronously`, async () => {
        const source = new ArrowDataSource(fs.promises.open(simpleFilePath, 'r'));
        expect(source.isSync()).toEqual(false);
        expect(source.isAsync()).toEqual(true);
        const pending = source.open();
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader).toBeInstanceOf(AsyncRecordBatchFileReader);
    });
}

function testStreamDataSource() {
    const buffer = simpleStreamData;
    it(`should return a ${RecordBatchStreamReader.name} when created with a Uint8Array`, () => {
        const source = new ArrowDataSource(buffer);
        expect(source.isSync()).toEqual(true);
        expect(source.isAsync()).toEqual(false);
        const reader = source.open();
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it(`should return a ${RecordBatchStreamReader.name} when created with an Iterable`, () => {
        const source = new ArrowDataSource(function* () { yield buffer; }());
        expect(source.isSync()).toEqual(true);
        expect(source.isAsync()).toEqual(false);
        const reader = source.open();
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it(`should return a ${RecordBatchStreamReader.name} when created with a Promise<Uint8Array> and should resolve asynchronously`, async () => {
        const source = new ArrowDataSource(Promise.resolve(buffer));
        expect(source.isSync()).toEqual(false);
        expect(source.isAsync()).toEqual(true);
        const pending = source.open();
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it(`should return an ${AsyncRecordBatchStreamReader.name} when created with an AsyncIterable and should resolve asynchronously`, async () => {
        const source = new ArrowDataSource(async function* () { yield buffer; }());
        expect(source.isSync()).toEqual(false);
        expect(source.isAsync()).toEqual(true);
        const pending = source.open();
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader).toBeInstanceOf(AsyncRecordBatchStreamReader);
    });
}
