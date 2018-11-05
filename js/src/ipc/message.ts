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
import { OptionallyAsync } from '../interfaces';

import { PADDING } from './magic';
import { MessageHeader } from '../enum';
import { Message } from './metadata/message';
import { toUint8Array } from '../util/buffer';
import { ITERATOR_DONE, IteratorBase, AsyncIteratorBase } from '../io/interfaces';

const invalidMessageType = (type: MessageHeader) => `Expected ${MessageHeader[type]} Message in stream, but was null or length 0.`;
const nullMessage = (type: MessageHeader) => `Header pointer of flatbuffer-encoded ${MessageHeader[type]} Message is null or length 0.`;
const invalidMessageMetadata = (expected: number, actual: number) => `Expected to read ${expected} metadata bytes, but only read ${actual}.`;
const invalidMessageBodyLength = (expected: number, actual: number) => `Expected to read ${expected} bytes for message body, but only read ${actual}.`;

export class MessageReader extends IteratorBase<Message, Iterator<ByteBuffer>> {
    public isSync(): this is MessageReader { return true; }
    public isAsync(): this is AsyncMessageReader { return false; }
    public next(): IteratorResult<Message> {
        let r;
        if ((r = this.readMetadataLength()).done) {
            return ITERATOR_DONE;
        }
        if ((r = this.readMetadata(r.value)).done) {
            return ITERATOR_DONE;
        }
        (r as IteratorResult<any>).value = r.value;
        return (<any> r) as IteratorResult<Message>;
    }
    public readMessageBody(bodyLength: number): Uint8Array {
        let r: IteratorResult<any> = super.next(bodyLength);
        if (!r.done && (r.value.capacity() < bodyLength)) {
            throw new Error(invalidMessageBodyLength(bodyLength, r.value.capacity()));
        }
        return toUint8Array(r.value);
    }
    public readMessage<T extends MessageHeader>(type?: T | null) {
        let r: IteratorResult<Message<T>>;
        if ((r = this.next()).done) { return null; }
        if ((type != null) && r.value.headerType !== type) {
            throw new Error(invalidMessageType(type));
        }
        return r.value;
    }
    public readSchema() {
        const type = MessageHeader.Schema;
        const message = this.readMessage(type);
        const schema = message && message.header();
        if (!message || !schema) {
            throw new Error(nullMessage(type));
        }
        return schema;
    }
    protected readMetadataLength(): IteratorResult<number> {
        let r: IteratorResult<any> = super.next(PADDING);
        r.done || (r.done = !(r.value = r.value.readInt32(0)));
        return r;
    }
    protected readMetadata(metadataLength: number): IteratorResult<Message> {
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
}

export class AsyncMessageReader extends AsyncIteratorBase<Message, AsyncIterator<ByteBuffer>> implements OptionallyAsync<MessageReader> {
    public isSync(): this is MessageReader { return false; }
    public isAsync(): this is AsyncMessageReader { return true; }
    public async next(): Promise<IteratorResult<Message>> {
        let r;
        if ((r = await this.readMetadataLength()).done) {
            return ITERATOR_DONE;
        }
        if ((r = await this.readMetadata(r.value)).done) {
            return ITERATOR_DONE;
        }
        (r as IteratorResult<any>).value = r.value;
        return (<any> r) as IteratorResult<Message>;
    }
    public async readMessageBody(bodyLength: number): Promise<Uint8Array> {
        let r: IteratorResult<any> = await super.next(bodyLength);
        if (!r.done && (r.value.capacity() < bodyLength)) {
            throw new Error(invalidMessageBodyLength(bodyLength, r.value.capacity()));
        }
        return toUint8Array(r.value);
    }
    public async readMessage<T extends MessageHeader>(type?: T | null) {
        let r: IteratorResult<Message<T>>;
        if ((r = await this.next()).done) { return null; }
        if ((type != null) && r.value.headerType !== type) {
            throw new Error(invalidMessageType(type));
        }
        return r.value;
    }
    public async readSchema() {
        const type = MessageHeader.Schema;
        const message = await this.readMessage(type);
        const schema = message && message.header();
        if (!message || !schema) {
            throw new Error(nullMessage(type));
        }
        return schema;
    }
    protected async readMetadataLength(): Promise<IteratorResult<number>> {
        let r: IteratorResult<any> = await super.next(PADDING);
        r.done || (r.done = !(r.value = r.value.readInt32(0)));
        return r;
    }
    protected async readMetadata(metadataLength: number): Promise<IteratorResult<Message>> {
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
}
