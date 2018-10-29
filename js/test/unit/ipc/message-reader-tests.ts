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
import { ReadableStream } from '@mattiasbuelens/web-streams-polyfill/ponyfill';

(global as any).ReadableStream = ReadableStream;

/* tslint:disable */
const nodeToWebStream = require('readable-stream-node-to-web');

import { MessageHeader } from '../../../src/enum';
import { Message } from '../../../src/ipc/message';

import { MessageReader } from '../../../src/ipc/message';
import { AsyncMessageReader } from '../../../src/ipc/message';

describe('readMessages', () => {
    it('should return a MessageReader when created with a Buffer', () => {
        expect(MessageReader.new(new Uint8Array())).toBeInstanceOf(MessageReader);
    });
    it('should return a MessageReader when created with an Iterable', () => {
        expect(MessageReader.new(function* () { yield new Uint8Array(); }())).toBeInstanceOf(MessageReader);
    });
    it('should return an AsyncMessageReader when created with a Promise', () => {
        expect(MessageReader.new(Promise.resolve(new Uint8Array()))).toBeInstanceOf(AsyncMessageReader);
    });
    it('should return an AsyncMessageReader when created with an AsyncIterable', () => {
        expect(MessageReader.new(async function* () { yield new Uint8Array(); }())).toBeInstanceOf(AsyncMessageReader);
    });
});

describe('MessageReader', () => {
    it('should read all messages from an Arrow Buffer stream', () => {

        const simple = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
        const reader = MessageReader.new(fs.readFileSync(simple));
        let r: IteratorResult<Message>;

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.Schema);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.RecordBatch);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.RecordBatch);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.RecordBatch);

        expect(reader.next().done).toBe(true);
    });
});

describe('AsyncMessageReader', () => {
    it('should read all messages from a NodeJS ReadableStream', async () => {

        const simple = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
        const reader = MessageReader.new(fs.createReadStream(simple));
        let r: IteratorResult<Message>;

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.Schema);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.RecordBatch);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.RecordBatch);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.RecordBatch);

        expect((await reader.next()).done).toBe(true);
    });

    it('should read all messages from a whatwg ReadableStream', async () => {

        const simple = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
        const reader = MessageReader.new(nodeToWebStream(fs.createReadStream(simple)));
        let r: IteratorResult<Message>;

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.Schema);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.RecordBatch);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.RecordBatch);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.headerType).toBe(MessageHeader.RecordBatch);

        expect((await reader.next()).done).toBe(true);
    });
});
