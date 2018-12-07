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

import { FileHandle } from '../../../src/io/interfaces';
import { RecordBatchReader }  from '../../../src/ipc/reader';
import { RecordBatchFileReader, AsyncRecordBatchFileReader } from '../../../src/ipc/reader';
import { RecordBatchStreamReader, AsyncRecordBatchStreamReader } from '../../../src/ipc/reader';

/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

const simpleFilePath = Path.resolve(__dirname, `../../data/cpp/file/simple.arrow`);
const simpleJSONPath = Path.resolve(__dirname, `../../data/json/simple.json`);
const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
const simpleFileData = fs.readFileSync(simpleFilePath) as Uint8Array;
const simpleJSONData = bignumJSONParse('' + fs.readFileSync(simpleJSONPath));
const simpleStreamData = fs.readFileSync(simpleStreamPath) as Uint8Array;

describe('RecordBatchReader#open', () => {
    testFileDataSource();
    testJSONDataSource();
    testStreamDataSource();
});

function testFileDataSource() {

    const syncBufferSource = () => simpleFileData;
    const asyncBufferSource = () => (async () => simpleFileData)();
    const asyncFileInputSource = () => fs.promises.open(simpleFilePath, 'r');
    const syncIteratorSource = () => (function* () { yield simpleFileData; })();
    const asyncIteratorSource = () => async function* () { yield simpleFileData; }();

    it(`should return a ${RecordBatchFileReader.name} when created with a Uint8Array`, () => {
        const reader = RecordBatchReader.open(syncBufferSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return a ${RecordBatchFileReader.name} when created with an Iterable`, () => {
        const reader = RecordBatchReader.open(syncIteratorSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return a ${RecordBatchFileReader.name} when created with a Promise<Uint8Array> and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.open(asyncBufferSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return a ${RecordBatchFileReader.name} when created with an AsyncIterable and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.open(asyncIteratorSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return an ${AsyncRecordBatchFileReader.name} when created with a Promise<FileHandle> and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.open(asyncFileInputSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchFileReader);
    });
    it(`should return an ${AsyncRecordBatchFileReader.name} when created with a FileHandle and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.open((await asyncFileInputSource()) as FileHandle);
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchFileReader);
    });
}

function testJSONDataSource() {

    const syncBufferSource = () => simpleJSONData;

    it(`should return a ${RecordBatchStreamReader.name} when created with a JSON object`, () => {
        const reader = RecordBatchReader.open(syncBufferSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
}

function testStreamDataSource() {

    const syncBufferSource = () => simpleStreamData;
    const asyncBufferSource = () => (async () => simpleStreamData)();
    const asyncFileInputSource = () => fs.promises.open(simpleStreamPath, 'r');
    const syncIteratorSource = () => (function* () { yield simpleStreamData; })();
    const asyncIteratorSource = () => async function* () { yield simpleStreamData; }();

    it(`should return a ${RecordBatchStreamReader.name} when created with a Uint8Array`, () => {
        const reader = RecordBatchReader.open(syncBufferSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it(`should return a ${RecordBatchStreamReader.name} when created with an Iterable`, () => {
        const reader = RecordBatchReader.open(syncIteratorSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it(`should return a ${RecordBatchStreamReader.name} when created with a Promise<Uint8Array> and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.open(asyncBufferSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it(`should return an ${AsyncRecordBatchStreamReader.name} when created with an AsyncIterable and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.open(asyncIteratorSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchStreamReader);
    });
    it(`should return an ${AsyncRecordBatchStreamReader.name} when created with a Promise<FileHandle> and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.open(asyncFileInputSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchStreamReader);
    });
    it(`should return an ${AsyncRecordBatchStreamReader.name} when created with a FileHandle and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.open(await asyncFileInputSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchStreamReader);
    });
}
