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

import { PADDING, MAGIC } from './magic';

// import { Schema, Long } from '../schema';
import { MessageHeader, MetadataVersion } from '../enum';
import { BufferReader, AsyncBufferReader } from './reader';
import { IteratorBase, AsyncIteratorBase, done } from './reader';

import { Readable } from 'stream';
import { ReadableStream } from 'whatwg-streams';
import { isPromise, isAsyncIterable, isReadableNodeStream, isReadableDOMStream } from '../util/compat';
import { fromIterable, fromAsyncIterable, fromReadableDOMStream, fromReadableNodeStream } from './stream';

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
    // static isSchema(m: Message): m is Schema { return m.headerType === MessageHeader.Schema; }
    // static isRecordBatch(m: Message): m is RecordBatchMetadata { return m.headerType === MessageHeader.RecordBatch; }
    // static isDictionaryBatch(m: Message): m is DictionaryBatch { return m.headerType === MessageHeader.DictionaryBatch; }
}

type TElement = ArrayBufferLike | ArrayBufferView | string;

export function readMessages<T extends TElement>(source: T | Iterable<T>): Iterator<Message>;
export function readMessages<T extends TElement>(source: Readable | ReadableStream<T>): AsyncIterator<Message>;
export function readMessages<T extends TElement>(source: PromiseLike<T> | AsyncIterable<T>): AsyncIterator<Message>;
export function readMessages<T extends TElement>(source: Readable | ReadableStream<T> | AsyncIterable<T> | PromiseLike<T> | Iterable<T> | T) {
    return (
        (isReadableDOMStream<T>(source)) ? (new AsyncMessageReader(fromReadableDOMStream<T>(source))) as AsyncIterator<Message> :
        (  isReadableNodeStream(source)) ? (new AsyncMessageReader(  fromReadableNodeStream(source))) as AsyncIterator<Message> :
        (    isAsyncIterable<T>(source)) ? (new AsyncMessageReader(    fromAsyncIterable<T>(source))) as AsyncIterator<Message> :
        (          isPromise<T>(source)) ? (new AsyncMessageReader(    fromAsyncIterable<T>(source))) as AsyncIterator<Message> :
                                           (     new MessageReader(    fromIterable<T>(source as T))) as      Iterator<Message>);
}

const invalidMessageBody = (expected: number, actual: number) => `Expected to read ${expected} bytes for message body, got ${actual}`;
const invalidMessageMetadata = (expected: number, actual: number) => `Expected to read ${expected} metadata bytes, but only read ${actual}`;

export class MessageReader extends IteratorBase<Message, BufferReader> {
    constructor(source: Iterator<Uint8Array>) {
        super(new BufferReader(source));
    }
    next() {
        let r, m: _Message;
        if ((r = this.readMetadataLength()).done) return done;
        if ((r = this.readMetadata(r.value)).done) return done;
        if ((r = this.readBody(m = r.value)).done) return done;
        (r as IteratorResult<any>).value = r.done ? null : new Message(m, r.value);
        return (r as any) as IteratorResult<Message>;
    }
    readMetadataLength(): IteratorResult<number> {
        let r: IteratorResult<any> = this.source.next(PADDING);
        r.done || (r.done = (r.value = r.value.readInt32(0)));
        return r;
    }
    readMetadata(metadataLength: number): IteratorResult<_Message> {
        let r: IteratorResult<any>
        if ((r = this.source.next(metadataLength)).done) return done;
        if ((r.value.capacity() < metadataLength)) {
            throw new Error(invalidMessageMetadata(metadataLength, r.value.capacity()));
        }
        const { value: bb } = r;
        bb.setPosition(bb.position() + PADDING);
        const message = _Message.getRootAsMessage(bb);
        bb.setPosition(bb.position() + metadataLength);
        r.done = (r.value = message) == null;
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

export class AsyncMessageReader extends AsyncIteratorBase<Message, AsyncBufferReader> {
    constructor(source: AsyncIterator<Uint8Array>) {
        super(new AsyncBufferReader(source));
    }
    async next() {
        let r, m: _Message;
        if ((r = await this.readMetadataLength()).done) return done;
        if ((r = await this.readMetadata(r.value)).done) return done;
        if ((r = await this.readBody(m = r.value)).done) return done;
        (r as IteratorResult<any>).value = r.done ? null : new Message(m, r.value);
        return (r as any) as IteratorResult<Message>;
    }
    async readMetadataLength(): Promise<IteratorResult<number>> {
        let r: IteratorResult<any> = await this.source.next(PADDING);
        r.done || (r.done = (r.value = r.value.readInt32(0)));
        return r;
    }
    async readMetadata(metadataLength: number): Promise<IteratorResult<_Message>> {
        let r: IteratorResult<any>
        if ((r = await this.source.next(metadataLength)).done) return done;
        if ((r.value.capacity() < metadataLength)) {
            throw new Error(invalidMessageMetadata(metadataLength, r.value.capacity()));
        }
        const { value: bb } = r;
        bb.setPosition(bb.position() + PADDING);
        const message = _Message.getRootAsMessage(bb);
        bb.setPosition(bb.position() + metadataLength);
        r.done = (r.value = message) == null;
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
