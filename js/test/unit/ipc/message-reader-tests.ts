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

import { MessageHeader } from '../../../src/enum';
import { Message } from '../../../src/ipc/message';

import { readMessages } from '../../../src/ipc/message';
import { MessageReader } from '../../../src/ipc/message';
import { AsyncMessageReader } from '../../../src/ipc/message';

describe('readMessages', () => {
    it('should return a MessageReader when called with a Buffer', () => {
        expect(readMessages(new Uint8Array())).toBeInstanceOf(MessageReader);
    });
    it('should return a MessageReader when called with an Iterable', () => {
        expect(readMessages(function* () { yield new Uint8Array(); }())).toBeInstanceOf(MessageReader);
    });
    it('should return an AsyncMessageReader when called with a Promise', () => {
        expect(readMessages(Promise.resolve(new Uint8Array()))).toBeInstanceOf(AsyncMessageReader);
    });
    it('should return an AsyncMessageReader when called with an AsyncIterable', () => {
        expect(readMessages(async function* () { yield new Uint8Array(); }())).toBeInstanceOf(AsyncMessageReader);
    });
});

describe('MessageReader', () => {
    it('should read all messages from an Arrow Buffer stream', () => {

        const simple = Path.resolve(__dirname, `../../data/cpp/stream/simple.arrow`);
        const reader = readMessages(fs.readFileSync(simple));
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
