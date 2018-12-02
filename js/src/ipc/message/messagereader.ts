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
import { MessageHeader } from '../../enum';
import { Message } from '../metadata/message';
import { ByteStream } from '../../io/interfaces';
import { toUint8Array } from '../../util/buffer';
import { AsyncMessageReader } from './asyncmessagereader';
import { ITERATOR_DONE, IteratorBase } from '../../io/interfaces';
import {
    PADDING,
    nullMessage,
    invalidMessageType,
    invalidMessageMetadata,
    invalidMessageBodyLength
} from './support';

export class MessageReader extends IteratorBase<Message, ByteStream<ByteBuffer>> {
    public isSync(): this is MessageReader { return true; }
    public isAsync(): this is AsyncMessageReader { return false; }
    public next(): IteratorResult<Message> {
        let r;
        if ((r = this.readMetadataLength()).done) { return ITERATOR_DONE; }
        if ((r = this.readMetadata(r.value)).done) { return ITERATOR_DONE; }
        return (<any> r) as IteratorResult<Message>;
    }
    public readMessage<T extends MessageHeader>(type?: T | null) {
        let r: IteratorResult<Message<T>>;
        if ((r = this.next()).done) { return null; }
        if ((type != null) && r.value.headerType !== type) {
            throw new Error(invalidMessageType(type));
        }
        return r.value;
    }
    public readMessageBody(bodyLength: number): Uint8Array {
        let bb = this.source.read(bodyLength);
        if (bb && (bb.capacity() < bodyLength)) {
            throw new Error(invalidMessageBodyLength(bodyLength, bb.capacity()));
        }
        return toUint8Array(bb);
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
        const bb = this.source.read(PADDING);
        const len = +(bb && bb.readInt32(0))!;
        return { done: len <= 0, value: len };
    }
    protected readMetadata(metadataLength: number): IteratorResult<Message> {
        let bb;
        if (!(bb = this.source.read(metadataLength))) { return ITERATOR_DONE; }
        if (bb.capacity() < metadataLength) {
            throw new Error(invalidMessageMetadata(metadataLength, bb.capacity()));
        }
        const m = Message.decode(bb);
        return { done: !m, value: m };
    }
}
