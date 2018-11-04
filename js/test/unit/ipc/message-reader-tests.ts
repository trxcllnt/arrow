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

import { Message } from '../../../src/ipc/metadata/message';

import { MessageReader } from '../../../src/ipc/message';
import { AsyncMessageReader } from '../../../src/ipc/message';
import { resolveInputFormat, ArrowStream, AsyncArrowStream } from '../../../src/ipc/input';

const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
const newMessageReader = (x: any) => new MessageReader(resolveInputFormat(x).resolve() as ArrowStream);
const newAsyncMessageReader = async (x: any) => new AsyncMessageReader(await resolveInputFormat(x).resolve() as AsyncArrowStream);

const zerosAndLengthPrefix = () => { const b = new Uint8Array(104); b[0] = 100; return b; };

describe('MessageReader InputResolver', () => {

    it('should return a MessageReader when created with a Buffer', () => {
        expect(newMessageReader(zerosAndLengthPrefix())).toBeInstanceOf(MessageReader);
    });
    it('should return a MessageReader when created with an Iterable', () => {
        expect(newMessageReader(function* () { yield zerosAndLengthPrefix(); }())).toBeInstanceOf(MessageReader);
    });
    it('should return an AsyncMessageReader when created with a Promise', async () => {
        expect(await newAsyncMessageReader(Promise.resolve(zerosAndLengthPrefix()))).toBeInstanceOf(AsyncMessageReader);
    });
    it('should return an AsyncMessageReader when created with an AsyncIterable', async () => {
        expect(await newAsyncMessageReader(async function* () { yield zerosAndLengthPrefix(); }())).toBeInstanceOf(AsyncMessageReader);
    });
});

describe('MessageReader', () => {
    it('should read all messages from an Arrow Buffer stream', () => {
        simpleStreamSyncMessageReaderTest(newMessageReader(fs.readFileSync(simpleStreamPath)));
    });

    function simpleStreamSyncMessageReaderTest(reader: MessageReader) {
        let r: IteratorResult<Message>;

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isSchema()).toBe(true);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);

        expect(reader.next().done).toBe(true);
    }
});

describe('AsyncMessageReader', () => {
    it('should read all messages from a NodeJS ReadableStream', async () => {
        await simpleStreamAsyncMessageReaderTest(await newAsyncMessageReader(fs.createReadStream(simpleStreamPath)));
    });

    // it('should read all messages from a whatwg ReadableStream', async () => {
    //     await simpleStreamAsyncMessageReaderTest(await newAsyncMessageReader(nodeToWebStream(fs.createReadStream(simpleStreamPath), { type: 'bytes' })));
    // });

    async function simpleStreamAsyncMessageReaderTest(reader: AsyncMessageReader) {
        let r: IteratorResult<Message>;

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isSchema()).toBe(true);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);

        expect((await reader.next()).done).toBe(true);
    }
});
