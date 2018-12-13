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
    RecordBatchReader,
    RecordBatchFileReader,
    RecordBatchStreamReader,
    AsyncRecordBatchFileReader,
    AsyncRecordBatchStreamReader
} from '../../../Arrow';

/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

const simpleJSONPath = Path.resolve(__dirname, `../../../data/json/simple.json`);
const simpleFilePath = Path.resolve(__dirname, `../../../data/cpp/file/simple.arrow`);
const simpleStreamPath = Path.resolve(__dirname, `../../../data/cpp/stream/simple.arrow`);
const simpleFileData = fs.readFileSync(simpleFilePath) as Uint8Array;
const simpleStreamData = fs.readFileSync(simpleStreamPath) as Uint8Array;
const simpleJSONData = bignumJSONParse('' + fs.readFileSync(simpleJSONPath));

describe('RecordBatchReader.from', () => {
    testFromFile();
    testFromJSON();
    testFromStream();
});

function testFromFile() {

    const syncBufferSource = () => simpleFileData;
    const asyncBufferSource = () => (async () => simpleFileData)();
    const asyncFileInputSource = () => fs.promises.open(simpleFilePath, 'r');
    const syncIteratorSource = () => (function* () { yield simpleFileData; })();
    const asyncIteratorSource = () => async function* () { yield simpleFileData; }();

    it(`should return a RecordBatchFileReader when created with a Uint8Array`, () => {
        const reader = RecordBatchReader.from(syncBufferSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return a RecordBatchFileReader when created with an Iterable`, () => {
        const reader = RecordBatchReader.from(syncIteratorSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return a RecordBatchFileReader when created with a Promise<Uint8Array> and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.from(asyncBufferSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return a RecordBatchFileReader when created with an AsyncIterable and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.from(asyncIteratorSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchFileReader);
    });
    it(`should return an AsyncRecordBatchFileReader when created with a Promise<fs.promises.FileHandle> and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.from(asyncFileInputSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchFileReader);
    });
    it(`should return an AsyncRecordBatchFileReader when created with an fs.promises.FileHandle and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.from((await asyncFileInputSource()) as import('fs').promises.FileHandle);
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchFileReader);
    });
}

function testFromJSON() {

    const syncBufferSource = () => simpleJSONData;

    it(`should return a RecordBatchStreamReader when created with a JSON object`, () => {
        const reader = RecordBatchReader.from(syncBufferSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
}

function testFromStream() {

    const syncBufferSource = () => simpleStreamData;
    const asyncBufferSource = () => (async () => simpleStreamData)();
    const asyncFileInputSource = () => fs.promises.open(simpleStreamPath, 'r');
    const syncIteratorSource = () => (function* () { yield simpleStreamData; })();
    const asyncIteratorSource = () => async function* () { yield simpleStreamData; }();

    it(`should return a RecordBatchStreamReader when created with a Uint8Array`, () => {
        const reader = RecordBatchReader.from(syncBufferSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it(`should return a RecordBatchStreamReader when created with an Iterable`, () => {
        const reader = RecordBatchReader.from(syncIteratorSource());
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it(`should return a RecordBatchStreamReader when created with a Promise<Uint8Array> and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.from(asyncBufferSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(true);
        expect(reader.isAsync()).toEqual(false);
        expect(reader).toBeInstanceOf(RecordBatchStreamReader);
    });
    it(`should return an AsyncRecordBatchStreamReader when created with an AsyncIterable and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.from(asyncIteratorSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchStreamReader);
    });
    it(`should return an AsyncRecordBatchStreamReader when created with a Promise<fs.promises.FileHandle> and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.from(asyncFileInputSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchStreamReader);
    });
    it(`should return an AsyncRecordBatchStreamReader when created with an fs.promises.FileHandle and should resolve asynchronously`, async () => {
        const pending = RecordBatchReader.from(await asyncFileInputSource());
        expect(pending).toBeInstanceOf(Promise);
        const reader = await pending;
        expect(reader.isSync()).toEqual(false);
        expect(reader.isAsync()).toEqual(true);
        expect(reader).toBeInstanceOf(AsyncRecordBatchStreamReader);
    });
}
