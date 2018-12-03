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

import { MessageHeader } from '../enum';
import { flatbuffers } from 'flatbuffers';
import ByteBuffer = flatbuffers.ByteBuffer;
import { Message } from './metadata/message';
import { toUint8Array } from '../util/buffer';
import { isFileHandle } from '../util/compat';
import {
    ITERATOR_DONE, ArrowJSON,
    ByteStream, AsyncByteStream,
    IteratorBase, AsyncIteratorBase,
    FileHandle, ArrowFile, AsyncArrowFile,
    ReadableDOMStream, ArrowStream, AsyncArrowStream,
} from '../io/interfaces';

export const invalidMessageType       = (type: MessageHeader) => `Expected ${MessageHeader[type]} Message in stream, but was null or length 0.`;
export const nullMessage              = (type: MessageHeader) => `Header pointer of flatbuffer-encoded ${MessageHeader[type]} Message is null or length 0.`;
export const invalidMessageMetadata   = (expected: number, actual: number) => `Expected to read ${expected} metadata bytes, but only read ${actual}.`;
export const invalidMessageBodyLength = (expected: number, actual: number) => `Expected to read ${expected} bytes for message body, but only read ${actual}.`;

export class MessageReader extends IteratorBase<Message, ByteStream<ByteBuffer>> {
    constructor(source: ArrowFile | ArrowStream | ArrayBufferView | Iterable<ArrayBufferView>) {
        let stream = source as ByteStream<ByteBuffer>;
        if (!((source instanceof ArrowFile) || (source instanceof ArrowStream))) {
            stream = ArrowStream.from(source) as ByteStream<ByteBuffer>;
        }
        super(stream);
    }
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
        const body = toUint8Array(bb);
        // Workaround bugs in fs.ReadStream's internal Buffer pooling
        // see: https://github.com/nodejs/node/issues/24817
        return body.byteOffset % 8 === 0 ? body : body.slice();
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

export class AsyncMessageReader extends AsyncIteratorBase<Message, AsyncByteStream<ByteBuffer>> {
    constructor(source: AsyncArrowFile | AsyncArrowStream | NodeJS.ReadableStream | ReadableDOMStream<ArrayBufferView> | AsyncIterable<ArrayBufferView>);
    constructor(source: FileHandle, byteLength?: number);
    constructor(source: any, byteLength?: number) {
        let stream = source as AsyncByteStream<ByteBuffer>;
        if (isFileHandle(source) && typeof byteLength === 'number') {
            stream = new AsyncArrowFile(source, byteLength);
        } else if (!((source instanceof AsyncArrowFile) || (source instanceof AsyncArrowStream))) {
            stream = AsyncArrowStream.from(source) as AsyncByteStream<ByteBuffer>;
        }
        super(stream);
    }
    public async next(): Promise<IteratorResult<Message>> {
        let r;
        if ((r = await this.readMetadataLength()).done) { return ITERATOR_DONE; }
        if ((r = await this.readMetadata(r.value)).done) { return ITERATOR_DONE; }
        return (<any> r) as IteratorResult<Message>;
    }
    public async readMessage<T extends MessageHeader>(type?: T | null) {
        let r: IteratorResult<Message<T>>;
        if ((r = await this.next()).done) { return null; }
        if ((type != null) && r.value.headerType !== type) {
            throw new Error(invalidMessageType(type));
        }
        return r.value;
    }
    public async readMessageBody(bodyLength: number): Promise<Uint8Array> {
        let bb = await this.source.read(bodyLength);
        if (bb && (bb.capacity() < bodyLength)) {
            throw new Error(invalidMessageBodyLength(bodyLength, bb.capacity()));
        }
        const body = toUint8Array(bb);
        // Workaround bugs in fs.ReadStream's internal Buffer pooling
        // see: https://github.com/nodejs/node/issues/24817
        return body.byteOffset % 8 === 0 ? body : body.slice();
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
        const bb = await this.source.read(PADDING);
        const len = +(bb && bb.readInt32(0))!;
        return { done: len <= 0, value: len };
    }
    protected async readMetadata(metadataLength: number): Promise<IteratorResult<Message>> {
        let bb;
        if (!(bb = await this.source.read(metadataLength))) { return ITERATOR_DONE; }
        if (bb.capacity() < metadataLength) {
            throw new Error(invalidMessageMetadata(metadataLength, bb.capacity()));
        }
        const m = Message.decode(bb);
        return { done: !m, value: m };
    }
}

export class JSONMessageReader extends MessageReader {
    private _schema = false;
    private _body: any[] = [];
    private _batchIndex = 0;
    private _dictionaryIndex = 0;
    constructor(private _json: ArrowJSON) {
        super(new Uint8Array(0));
    }
    public next() {
        const { _json, _batchIndex, _dictionaryIndex } = this;
        const numBatches = _json.batches.length;
        const numDictionaries = _json.dictionaries.length;
        if (!this._schema) {
            this._schema = true;
            const message = Message.fromJSON(_json.schema, MessageHeader.Schema);
            return { value: message, done: _batchIndex >= numBatches && _dictionaryIndex >= numDictionaries };
        }
        if (_dictionaryIndex < numDictionaries) {
            const batch = _json.dictionaries[this._dictionaryIndex++];
            this._body = batch['data']['columns'];
            const message = Message.fromJSON(batch, MessageHeader.DictionaryBatch);
            return { done: false, value: message };
        }
        if (_batchIndex < numBatches) {
            const batch = _json.batches[this._batchIndex++];
            this._body = batch['columns'];
            const message = Message.fromJSON(batch, MessageHeader.RecordBatch);
            return { done: false, value: message };
        }
        this._body = [];
        return ITERATOR_DONE;
    }
    public readMessageBody(_bodyLength?: number) {
        return flattenDataSources(this._body) as any;
        function flattenDataSources(xs: any[]): any[][] {
            return (xs || []).reduce<any[][]>((buffers, column: any) => [
                ...buffers,
                ...(column['VALIDITY'] && [column['VALIDITY']] || []),
                ...(column['OFFSET'] && [column['OFFSET']] || []),
                ...(column['TYPE'] && [column['TYPE']] || []),
                ...(column['DATA'] && [column['DATA']] || []),
                ...flattenDataSources(column['children'])
            ], [] as any[][]);
        }
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
}

export const PADDING = 4;
export const MAGIC_STR = 'ARROW1';
export const MAGIC = new Uint8Array(MAGIC_STR.length);

for (let i = 0; i < MAGIC_STR.length; i += 1 | 0) {
    MAGIC[i] = MAGIC_STR.charCodeAt(i);
}

export function checkForMagicArrowString(buffer: Uint8Array, index = 0) {
    for (let i = -1, n = MAGIC.length; ++i < n;) {
        if (MAGIC[i] !== buffer[index + i]) {
            return false;
        }
    }
    return true;
}

export function isValidArrowFile(bb: ByteBuffer) {
    let fileLength = bb.capacity(), footerLength: number, lengthOffset: number;
    if ((fileLength < magicX2AndPadding /*                                  Arrow buffer too small */) ||
        (!checkForMagicArrowString(bb.bytes(), 0) /*                        Missing magic start    */) ||
        (!checkForMagicArrowString(bb.bytes(), fileLength - magicLength) /* Missing magic end      */) ||
        (/*                                                                 Invalid footer length  */
        (footerLength = bb.readInt32(lengthOffset = fileLength - magicAndPadding)) < 1 &&
        (footerLength + lengthOffset > fileLength))) {
        return false;
    }
    return true;
}

export const magicLength = MAGIC.length;
export const magicAndPadding = magicLength + PADDING;
export const magicX2AndPadding = magicLength * 2 + PADDING;
