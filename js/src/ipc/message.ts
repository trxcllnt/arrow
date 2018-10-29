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

import * as Message_ from '../fb/Message';
import { flatbuffers } from 'flatbuffers';
import ByteBuffer = flatbuffers.ByteBuffer;
import _Message = Message_.org.apache.arrow.flatbuf.Message;
type ReadableStream<R = any> = import('whatwg-streams').ReadableStream<R>;

import { Readable } from 'stream';
import { PADDING } from './magic';
import { Schema, Long } from '../schema';
import { MessageHeader, MetadataVersion } from '../enum';
import {
    ITERATOR_DONE,
    IteratorBase, AsyncIteratorBase,
    ByteStream
} from './stream';

export class Message {
    public readonly body: ByteBuffer;
    public readonly metadata: ByteBuffer;
    protected readonly _message: _Message;
    constructor(message: _Message, body: ByteBuffer) {
        this.body = body;
        this._message = message;
        this.metadata = message.bb;
    }
    public get headerType(): MessageHeader { return this.type; }
    public get type(): MessageHeader { return this._message.headerType(); }
    public get version(): MetadataVersion { return this._message.version(); }
    public get bodyLength(): number { return this._message.bodyLength().low; }
    public header<T extends flatbuffers.Table>(obj: T) {
        return this._message.header(obj);
    }
}

export class Footer {
    constructor(public dictionaryBatches: FileBlock[],
                public recordBatches: FileBlock[],
                public schema: Schema) {}
}

export class FileBlock {
    public offset: number;
    public bodyLength: number;
    constructor(public metaDataLength: number, bodyLength: Long | number, offset: Long | number) {
        this.offset = typeof offset === 'number' ? offset : offset.low;
        this.bodyLength = typeof bodyLength === 'number' ? bodyLength : bodyLength.low;
    }
}

export class BufferMetadata {
    public offset: number;
    public length: number;
    constructor(offset: Long | number, length: Long | number) {
        this.offset = typeof offset === 'number' ? offset : offset.low;
        this.length = typeof length === 'number' ? length : length.low;
    }
}

export class FieldMetadata {
    public length: number;
    public nullCount: number;
    constructor(length: Long | number, nullCount: Long | number) {
        this.length = typeof length === 'number' ? length : length.low;
        this.nullCount = typeof nullCount === 'number' ? nullCount : nullCount.low;
    }
}

type TElement = ArrayBufferLike | ArrayBufferView | string;
const invalidMessageBody = (expected: number, actual: number) => `Expected to read ${expected} bytes for message body, got ${actual}`;
const invalidMessageMetadata = (expected: number, actual: number) => `Expected to read ${expected} metadata bytes, but only read ${actual}`;

export class MessageReader extends IteratorBase<Message, Iterator<ByteBuffer>> {

    static new<T extends TElement>(source: T | Iterable<T>): MessageReader;
    static new<T extends TElement>(source: Readable | ReadableStream<T> | PromiseLike<T> | AsyncIterable<T>): AsyncMessageReader;
    static new<T extends TElement>(source: any) {
        const stream = ByteStream.new<T>(source);
        return stream.isAsync()
            ? new AsyncMessageReader(stream)
                 : new MessageReader(stream);
    }

    next() {
        let r, m: _Message;
        if ((r = this.readMetadataLength()).done) { return ITERATOR_DONE; }
        if ((r = this.readMetadata(r.value)).done) { return ITERATOR_DONE; }
        if ((r = this.readBody(m = r.value)).done) { return ITERATOR_DONE; }
        (r as IteratorResult<any>).value = new Message(m, r.value);
        return (r as IteratorResult<any>) as IteratorResult<Message>;
    }
    readMetadataLength(): IteratorResult<number> {
        let r: IteratorResult<any> = this.source.next(PADDING);
        r.done || (r.done = !(r.value = r.value.readInt32(0)));
        return r;
    }
    readMetadata(metadataLength: number): IteratorResult<_Message> {
        let r: IteratorResult<any>;
        if ((r = this.source.next(metadataLength)).done) { return ITERATOR_DONE; }
        if ((r.value.capacity() < metadataLength)) {
            throw new Error(invalidMessageMetadata(metadataLength, r.value.capacity()));
        }
        r.done = !(r.value = _Message.getRootAsMessage(r.value));
        return r;
    }
    readBody(message: _Message): IteratorResult<ByteBuffer> {
        let { low: bodyLength } = message.bodyLength();
        let r: IteratorResult<any> = this.source.next(bodyLength);
        if ((r.value.capacity() < bodyLength)) {
            throw new Error(invalidMessageBody(bodyLength, r.value.capacity()));
        }
        return r;
    }
}

export class AsyncMessageReader extends AsyncIteratorBase<Message, AsyncIterator<ByteBuffer>> {
    async next() {
        let r, m: _Message;
        if ((r = await this.readMetadataLength()).done) { return ITERATOR_DONE; }
        if ((r = await this.readMetadata(r.value)).done) { return ITERATOR_DONE; }
        if ((r = await this.readBody(m = r.value)).done) { return ITERATOR_DONE; }
        (r as IteratorResult<any>).value = new Message(m, r.value);
        return (r as IteratorResult<any>) as IteratorResult<Message>;
    }
    async readMetadataLength(): Promise<IteratorResult<number>> {
        let r: IteratorResult<any> = await this.source.next(PADDING);
        r.done || (r.done = !(r.value = r.value.readInt32(0)));
        return r;
    }
    async readMetadata(metadataLength: number): Promise<IteratorResult<_Message>> {
        let r: IteratorResult<any>;
        if ((r = await this.source.next(metadataLength)).done) { return ITERATOR_DONE; }
        if ((r.value.capacity() < metadataLength)) {
            throw new Error(invalidMessageMetadata(metadataLength, r.value.capacity()));
        }
        r.done = !(r.value = _Message.getRootAsMessage(r.value));
        return r as IteratorResult<_Message>;
    }
    async readBody(message: _Message): Promise<IteratorResult<ByteBuffer>> {
        let { low: bodyLength } = message.bodyLength();
        let r: IteratorResult<any> = await this.source.next(bodyLength);
        if ((r.value.capacity() < bodyLength)) {
            throw new Error(invalidMessageBody(bodyLength, r.value.capacity()));
        }
        return r;
    }
}
