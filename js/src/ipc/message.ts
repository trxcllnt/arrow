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
import { isFileHandle } from '../util/compat';
import { AsyncRandomAccessFile } from '../io/file';
import { toUint8Array, ArrayBufferViewInput } from '../util/buffer';
import { ByteStream, ReadableSource, AsyncByteStream } from '../io/stream';
import { ArrowJSON, ArrowJSONLike, ITERATOR_DONE, FileHandle } from '../io/interfaces';

const invalidMessageType       = (type: MessageHeader) => `Expected ${MessageHeader[type]} Message in stream, but was null or length 0.`;
const nullMessage              = (type: MessageHeader) => `Header pointer of flatbuffer-encoded ${MessageHeader[type]} Message is null or length 0.`;
const invalidMessageMetadata   = (expected: number, actual: number) => `Expected to read ${expected} metadata bytes, but only read ${actual}.`;
const invalidMessageBodyLength = (expected: number, actual: number) => `Expected to read ${expected} bytes for message body, but only read ${actual}.`;

export class MessageReader implements IterableIterator<Message> {
    protected source: ByteStream;
    constructor(source: ByteStream | ArrayBufferViewInput | Iterable<ArrayBufferViewInput>) {
        this.source = source instanceof ByteStream ? source : new ByteStream(source);
    }
    public [Symbol.iterator](): IterableIterator<Message> { return this as IterableIterator<Message>; }
    public next(): IteratorResult<Message> {
        let r;
        if ((r = this.readMetadataLength()).done) { return ITERATOR_DONE; }
        if ((r = this.readMetadata(r.value)).done) { return ITERATOR_DONE; }
        return (<any> r) as IteratorResult<Message>;
    }
    public throw(value?: any) { return this.source.throw(value); }
    public return(value?: any) { return this.source.return(value); }
    public readMessage<T extends MessageHeader>(type?: T | null) {
        let r: IteratorResult<Message<T>>;
        if ((r = this.next()).done) { return null; }
        if ((type != null) && r.value.headerType !== type) {
            throw new Error(invalidMessageType(type));
        }
        return r.value;
    }
    public readMessageBody(bodyLength: number): Uint8Array {
        if (bodyLength <= 0) { return new Uint8Array(0); }
        const buf = toUint8Array(this.source.read(bodyLength));
        if (buf.byteLength < bodyLength) {
            throw new Error(invalidMessageBodyLength(bodyLength, buf.byteLength));
        }
        // Work around bugs in fs.ReadStream's internal Buffer pooling, see: https://github.com/nodejs/node/issues/24817
        return buf.byteOffset % 8 === 0 ? buf : buf.slice();
    }
    public readSchema(throwIfNull = false) {
        const type = MessageHeader.Schema;
        const message = this.readMessage(type);
        const schema = message && message.header();
        if (throwIfNull && !schema) {
            throw new Error(nullMessage(type));
        }
        return schema;
    }
    protected readMetadataLength(): IteratorResult<number> {
        const buf = this.source.read(PADDING);
        const bb = buf && new ByteBuffer(buf);
        const len = +(bb && bb.readInt32(0))!;
        return { done: len <= 0, value: len };
    }
    protected readMetadata(metadataLength: number): IteratorResult<Message> {
        const buf = this.source.read(metadataLength);
        if (!buf) { return ITERATOR_DONE; }
        if (buf.byteLength < metadataLength) {
            throw new Error(invalidMessageMetadata(metadataLength, buf.byteLength));
        }
        return { done: false, value: Message.decode(buf) };
    }
}

export class AsyncMessageReader implements AsyncIterableIterator<Message> {
    protected source: AsyncByteStream;
    constructor(source: ReadableSource<Uint8Array>);
    constructor(source: FileHandle, byteLength?: number);
    constructor(source: any, byteLength?: number) {
        this.source = source instanceof AsyncByteStream ? source
            : (isFileHandle(source) && typeof byteLength === 'number')
            ? new AsyncRandomAccessFile(source, byteLength)
            : new AsyncByteStream(source);
    }
    public [Symbol.asyncIterator](): AsyncIterableIterator<Message> { return this as AsyncIterableIterator<Message>; }
    public async next(): Promise<IteratorResult<Message>> {
        let r;
        if ((r = await this.readMetadataLength()).done) { return ITERATOR_DONE; }
        if ((r = await this.readMetadata(r.value)).done) { return ITERATOR_DONE; }
        return (<any> r) as IteratorResult<Message>;
    }
    public async throw(value?: any) { return await this.source.throw(value); }
    public async return(value?: any) { return await this.source.return(value); }
    public async readMessage<T extends MessageHeader>(type?: T | null) {
        let r: IteratorResult<Message<T>>;
        if ((r = await this.next()).done) { return null; }
        if ((type != null) && r.value.headerType !== type) {
            throw new Error(invalidMessageType(type));
        }
        return r.value;
    }
    public async readMessageBody(bodyLength: number): Promise<Uint8Array> {
        if (bodyLength <= 0) { return new Uint8Array(0); }
        const buf = toUint8Array(await this.source.read(bodyLength));
        if (buf.byteLength < bodyLength) {
            throw new Error(invalidMessageBodyLength(bodyLength, buf.byteLength));
        }
        // Work around bugs in fs.ReadStream's internal Buffer pooling, see: https://github.com/nodejs/node/issues/24817
        return buf.byteOffset % 8 === 0 ? buf : buf.slice();
    }
    public async readSchema(throwIfNull = false) {
        const type = MessageHeader.Schema;
        const message = await this.readMessage(type);
        const schema = message && message.header();
        if (throwIfNull && !schema) {
            throw new Error(nullMessage(type));
        }
        return schema;
    }
    protected async readMetadataLength(): Promise<IteratorResult<number>> {
        const buf = await this.source.read(PADDING);
        const bb = buf && new ByteBuffer(buf);
        const len = +(bb && bb.readInt32(0))!;
        return { done: len <= 0, value: len };
    }
    protected async readMetadata(metadataLength: number): Promise<IteratorResult<Message>> {
        const buf = await this.source.read(metadataLength);
        if (!buf) { return ITERATOR_DONE; }
        if (buf.byteLength < metadataLength) {
            throw new Error(invalidMessageMetadata(metadataLength, buf.byteLength));
        }
        return { done: false, value: Message.decode(buf) };
    }
}

export class JSONMessageReader extends MessageReader {
    private _schema = false;
    private _json: ArrowJSON;
    private _body: any[] = [];
    private _batchIndex = 0;
    private _dictionaryIndex = 0;
    constructor(source: ArrowJSON | ArrowJSONLike) {
        super(new Uint8Array(0));
        this._json = source instanceof ArrowJSON ? source : new ArrowJSON(source);
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
                ...(column['TYPE'] && [column['TYPE']] || []),
                ...(column['OFFSET'] && [column['OFFSET']] || []),
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
