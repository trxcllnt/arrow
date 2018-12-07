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
import { nodeToDOMStream } from './util';

import { Message } from '../../../src/ipc/metadata/message';
import { MessageReader, AsyncMessageReader } from '../../../src/ipc/message';

const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);

describe('MessageReader', () => {

    it('should read all messages from an Arrow Buffer stream', () => {
        simpleStreamSyncMessageReaderTest(new MessageReader(fs.readFileSync(simpleStreamPath)));
    });

    function simpleStreamSyncMessageReaderTest(reader: MessageReader) {
        let r: IteratorResult<Message>;

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isSchema()).toBe(true);
        expect(reader.readMessageBody(r.value.bodyLength)).toBeInstanceOf(Uint8Array);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);
        expect(reader.readMessageBody(r.value.bodyLength)).toBeInstanceOf(Uint8Array);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);
        expect(reader.readMessageBody(r.value.bodyLength)).toBeInstanceOf(Uint8Array);

        r = reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);
        expect(reader.readMessageBody(r.value.bodyLength)).toBeInstanceOf(Uint8Array);

        expect(reader.next().done).toBe(true);

        reader.return();
    }
});

describe('AsyncMessageReader', () => {

    it('should read all messages from a NodeJS ReadableStream', async () => {
        await simpleStreamAsyncMessageReaderTest(new AsyncMessageReader(fs.createReadStream(simpleStreamPath)));
    });

    it('should read all messages from a whatwg ReadableStream', async () => {
        await simpleStreamAsyncMessageReaderTest(new AsyncMessageReader(
            nodeToDOMStream(fs.createReadStream(simpleStreamPath))));
    });

    it('should read all messages from a whatwg ReadableByteStream', async () => {
        await simpleStreamAsyncMessageReaderTest(new AsyncMessageReader(
            nodeToDOMStream(fs.createReadStream(simpleStreamPath), { type: 'bytes' })));
    });

    async function simpleStreamAsyncMessageReaderTest(reader: AsyncMessageReader) {
        let r: IteratorResult<Message>;

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isSchema()).toBe(true);
        expect(await reader.readMessageBody(r.value.bodyLength)).toBeInstanceOf(Uint8Array);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);
        expect(await reader.readMessageBody(r.value.bodyLength)).toBeInstanceOf(Uint8Array);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);
        expect(await reader.readMessageBody(r.value.bodyLength)).toBeInstanceOf(Uint8Array);

        r = await reader.next();
        expect(r.done).toBe(false);
        expect(r.value).toBeInstanceOf(Message);
        expect(r.value.isRecordBatch()).toBe(true);
        expect(await reader.readMessageBody(r.value.bodyLength)).toBeInstanceOf(Uint8Array);

        expect((await reader.next()).done).toBe(true);

        await reader.return();
    }
});
