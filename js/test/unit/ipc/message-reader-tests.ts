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
import { MessageReader, AsyncMessageReader } from '../../Arrow';
import { nodeToDOMStream, chunkedIterable, asyncChunkedIterable } from './util';
import {
    simpleStreamSyncMessageReaderTest,
    simpleStreamAsyncMessageReaderTest,
} from './validate';

const simpleStreamPath = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
const simpleStreamData = fs.readFileSync(simpleStreamPath);

describe('MessageReader', () => {
    it('should read all messages from an Arrow Buffer stream', () => {
        simpleStreamSyncMessageReaderTest(new MessageReader(simpleStreamData));
    });
    it('should read all messages from an Iterable that yields buffers of Arrow messages in memory', () => {
        simpleStreamSyncMessageReaderTest(new MessageReader(chunkedIterable(simpleStreamData)));
    });
});

describe('AsyncMessageReader', () => {
    it('should read all messages from a whatwg ReadableStream', async () => {
        await simpleStreamAsyncMessageReaderTest(new AsyncMessageReader(
            nodeToDOMStream(fs.createReadStream(simpleStreamPath))));
    });
    it('should read all messages from a whatwg ReadableByteStream', async () => {
        await simpleStreamAsyncMessageReaderTest(new AsyncMessageReader(
            nodeToDOMStream(fs.createReadStream(simpleStreamPath), { type: 'bytes' })));
    });
    it('should read all messages from a NodeJS ReadableStream', async () => {
        await simpleStreamAsyncMessageReaderTest(new AsyncMessageReader(fs.createReadStream(simpleStreamPath)));
    });
    it('should read all messages from a Promise<Buffer> of Arrow messages in memory', async () => {
        await simpleStreamAsyncMessageReaderTest(new AsyncMessageReader((async () => simpleStreamData)()));
    });
    it('should read all messages from an AsyncIterable that yields buffers of Arrow messages in memory', async () => {
        await simpleStreamAsyncMessageReaderTest(new AsyncMessageReader(asyncChunkedIterable(simpleStreamData)));
    });
});
