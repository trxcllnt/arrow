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

import { flatbuffers } from 'flatbuffers';
import ByteBuffer = flatbuffers.ByteBuffer;

import { PADDING } from './magic';
import { IsSync } from '../interfaces';
import { MessageHeader } from '../enum';
import { Message } from './metadata/message';
import { toUint8Array } from '../util/buffer';
import { ITERATOR_DONE, IteratorBase, AsyncIteratorBase } from '../io/interfaces';

const invalidMessageType = (type: MessageHeader) => `Expected ${MessageHeader[type]} Message in stream, but was null or length 0.`;
const nullMessage = (type: MessageHeader) => `Header pointer of flatbuffer-encoded ${MessageHeader[type]} Message is null or length 0.`;
const invalidMessageMetadata = (expected: number, actual: number) => `Expected to read ${expected} metadata bytes, but only read ${actual}.`;
const invalidMessageBodyLength = (expected: number, actual: number) => `Expected to read ${expected} bytes for message body, but only read ${actual}.`;

export class MessageReader extends IteratorBase<Message, Iterator<ByteBuffer>> {
    isSync(): this is MessageReader { return true; }
    isAsync(): this is AsyncMessageReader { return false; }
    next() {
        let r, message: Message;
        if ((r = this.readMetadataLength()).done) {
            return ITERATOR_DONE;
        }
        if ((r = this.readMetadata(r.value)).done) {
            return ITERATOR_DONE;
        }
        if ((r = this.readBody((message = r.value).bodyLength)).done) {
            return ITERATOR_DONE;
        }
        message.body = toUint8Array(r.value);
        (r as IteratorResult<any>).value = message;
        return (<any> r) as IteratorResult<Message>;
    }
    readMetadataLength(): IteratorResult<number> {
        let r: IteratorResult<any> = super.next(PADDING);
        r.done || (r.done = !(r.value = r.value.readInt32(0)));
        return r;
    }
    readMetadata(metadataLength: number): IteratorResult<Message> {
        let r: IteratorResult<any>;
        if ((r = super.next(metadataLength)).done) {
            return ITERATOR_DONE;
        }
        if ((r.value.capacity() < metadataLength)) {
            throw new Error(invalidMessageMetadata(metadataLength, r.value.capacity()));
        }
        r.done = !(r.value = Message.decode(r.value));
        return r;
    }
    readBody(bodyLength: number): IteratorResult<ByteBuffer> {
        let r: IteratorResult<any> = super.next(bodyLength);
        if (!r.done && (r.value.capacity() < bodyLength)) {
            throw new Error(invalidMessageBodyLength(bodyLength, r.value.capacity()));
        }
        return r;
    }
    readMessage<T extends MessageHeader>(type: T) {
        let r: IteratorResult<Message<T>>;
        if ((r = this.next()).done) { return null; }
        if (r.value.headerType !== type) {
            throw new Error(invalidMessageType(type));
        }
        return r.value;
    }
    readSchema() {
        const type = MessageHeader.Schema;
        const message = this.readMessage(type);
        const schema = message && message.header();
        if (!message || !schema) {
            throw new Error(nullMessage(type));
        }
        return schema;
    }
}

export class AsyncMessageReader extends AsyncIteratorBase<Message, AsyncIterator<ByteBuffer>> implements IsSync<MessageReader> {
    isSync(): this is MessageReader { return false; }
    isAsync(): this is AsyncMessageReader { return true; }
    async next() {
        let r, message: Message;
        if ((r = await this.readMetadataLength()).done) {
            return ITERATOR_DONE;
        }
        if ((r = await this.readMetadata(r.value)).done) {
            return ITERATOR_DONE;
        }
        if ((r = await this.readBody((message = r.value).bodyLength)).done) {
            return ITERATOR_DONE;
        }
        message.body = toUint8Array(r.value);
        (r as IteratorResult<any>).value = message;
        return (<any> r) as IteratorResult<Message>;
    }
    async readMetadataLength(): Promise<IteratorResult<number>> {
        let r: IteratorResult<any> = await super.next(PADDING);
        r.done || (r.done = !(r.value = r.value.readInt32(0)));
        return r;
    }
    async readMetadata(metadataLength: number): Promise<IteratorResult<Message>> {
        let r: IteratorResult<any>;
        if ((r = await super.next(metadataLength)).done) {
            return ITERATOR_DONE;
        }
        if ((r.value.capacity() < metadataLength)) {
            throw new Error(invalidMessageMetadata(metadataLength, r.value.capacity()));
        }
        r.done = !(r.value = Message.decode(r.value));
        return r as IteratorResult<Message>;
    }
    async readBody(bodyLength: number): Promise<IteratorResult<ByteBuffer>> {
        let r: IteratorResult<any> = await super.next(bodyLength);
        if (!r.done && (r.value.capacity() < bodyLength)) {
            throw new Error(invalidMessageBodyLength(bodyLength, r.value.capacity()));
        }
        return r;
    }
    async readMessage<T extends MessageHeader>(type: T) {
        let r: IteratorResult<Message<T>>;
        if ((r = await this.next()).done) { return null; }
        if (r.value.headerType !== type) {
            throw new Error(invalidMessageType(type));
        }
        return r.value;
    }
    async readSchema() {
        const type = MessageHeader.Schema;
        const message = await this.readMessage(type);
        const schema = message && message.header();
        if (!message || !schema) {
            throw new Error(nullMessage(type));
        }
        return schema;
    }
}
