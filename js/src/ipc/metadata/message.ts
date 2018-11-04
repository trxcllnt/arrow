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
import * as Schema_ from '../../fb/Schema';
import * as Message_ from '../../fb/Message';

import Long = flatbuffers.Long;
import ByteBuffer = flatbuffers.ByteBuffer;
import _Buffer = Schema_.org.apache.arrow.flatbuf.Buffer;
import _Schema = Schema_.org.apache.arrow.flatbuf.Schema;
import _Message = Message_.org.apache.arrow.flatbuf.Message;
import _FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;
import _RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;
import _DictionaryBatch = Message_.org.apache.arrow.flatbuf.DictionaryBatch;

import './schema';
import { Schema } from '../../schema';
import { toUint8Array } from '../../util/buffer';
import { ArrayBufferViewInput } from '../../util/buffer';
import { MessageHeader, MetadataVersion } from '../../enum';

type MessageHeaderDecoder = <T extends MessageHeader>() => T extends MessageHeader.Schema ? Schema
                                                         : T extends MessageHeader.RecordBatch ? RecordBatch
                                                         : T extends MessageHeader.DictionaryBatch ? DictionaryBatch : never;

export class Message<T extends MessageHeader = any> {

    static decode(buf: ArrayBufferViewInput) {
        buf = new ByteBuffer(toUint8Array(buf));
        const _message = _Message.getRootAsMessage(buf);
        const bodyLength: Long = _message.bodyLength()!;
        const version: MetadataVersion = _message.version();
        const headerType: MessageHeader = _message.headerType();
        const message = new Message(bodyLength, version, headerType);
        message._createHeader = decodeMessageHeader(_message, headerType);
        message._metadataLength = buf.capacity();
        return message;
    }


    // @ts-ignore
    public body: Uint8Array;
    public readonly headerType: T;
    public readonly bodyLength: number;
    public readonly version: MetadataVersion;
    public get type() { return this.headerType; }
    public get metadataLength() { return this._metadataLength; }
    protected _metadataLength = 0;
    // @ts-ignore
    protected _createHeader: MessageHeaderDecoder;
    public header() {
        return this._createHeader ? this._createHeader<T>() : null;
    }
    public isSchema(): this is Message<MessageHeader.Schema> {
        return this.headerType === MessageHeader.Schema;
    }
    public isRecordBatch(): this is Message<MessageHeader.RecordBatch> {
        return this.headerType === MessageHeader.RecordBatch;
    }
    public isDictionaryBatch(): this is Message<MessageHeader.DictionaryBatch> {
        return this.headerType === MessageHeader.DictionaryBatch;
    }

    constructor(bodyLength: Long | number, version: MetadataVersion, headerType: T) {
        this.version = version;
        this.headerType = headerType;
        this.body = new Uint8Array(0);
        this.bodyLength = typeof bodyLength === 'number' ? bodyLength : bodyLength.low;
    }
}

export class RecordBatch {

    static decode(batch: _RecordBatch, version = MetadataVersion.V4) {
        return new RecordBatch(batch.length(), decodeFieldNodes(batch), decodeBuffers(batch, version));
    }

    public readonly length: number;
    public readonly nodes: FieldNode[];
    public readonly buffers: BufferRegion[];

    constructor(length: Long | number, nodes: FieldNode[], buffers: BufferRegion[]) {
        this.nodes = nodes;
        this.buffers = buffers;
        this.length = typeof length === 'number' ? length : length.low;
    }
}

export class DictionaryBatch {

    private static atomicDictionaryId = 0;

    static decode(batch: _DictionaryBatch, version = MetadataVersion.V4) {
        return new DictionaryBatch(RecordBatch.decode(batch.data()!, version), batch.id(), batch.isDelta());
    }
    
    public readonly id: number;
    public readonly isDelta: boolean;
    public readonly data: RecordBatch;
    public get nodes(): FieldNode[] { return this.data.nodes; }
    public get buffers(): BufferRegion[] { return this.data.buffers; }
    public static getId() { return DictionaryBatch.atomicDictionaryId++; }

    constructor(data: RecordBatch, id: Long | number, isDelta: boolean = false) {
        this.data = data;
        this.isDelta = isDelta;
        this.id = typeof id === 'number' ? id : id.low;
    }
}

export class BufferRegion {

    static decode(b: _Buffer) {
        return new BufferRegion(b.offset(), b.length());
    }

    public offset: number;
    public length: number;
    constructor(offset: Long | number, length: Long | number) {
        this.offset = typeof offset === 'number' ? offset : offset.low;
        this.length = typeof length === 'number' ? length : length.low;
    }
}

export class FieldNode {

    static decode(f: _FieldNode) {
        return new FieldNode(f.length(), f.nullCount());
    }
    
    public length: number;
    public nullCount: number;
    constructor(length: Long | number, nullCount: Long | number) {
        this.length = typeof length === 'number' ? length : length.low;
        this.nullCount = typeof nullCount === 'number' ? nullCount : nullCount.low;
    }
}

function decodeMessageHeader(message: _Message, type: MessageHeader) {
    return (() => {
        switch (type) {
            case MessageHeader.Schema: return Schema.decode(message.header(new _Schema())!, new Map());
            case MessageHeader.RecordBatch: return RecordBatch.decode(message.header(new _RecordBatch())!, message.version());
            case MessageHeader.DictionaryBatch: return DictionaryBatch.decode(message.header(new _DictionaryBatch())!, message.version());
        }
        // return null;
        throw new Error(`Unrecognized Message type: { name: ${MessageHeader[type]}, type: ${type} }`);
    }) as MessageHeaderDecoder;
}

function decodeFieldNodes(batch: _RecordBatch) {
    return Array.from(
        { length: batch.nodesLength() },
        (_, i) => batch.nodes(i)!
    ).filter(Boolean).map(FieldNode.decode);
}

function decodeBuffers(batch: _RecordBatch, version: MetadataVersion) {
    return Array.from(
        { length: batch.buffersLength() },
        (_, i) => batch.buffers(i)!
    ).filter(Boolean).map(v3Compat(version, BufferRegion.decode));
}

function v3Compat(version: MetadataVersion, decode: (buffer: _Buffer) => BufferRegion) {
    return (buffer: _Buffer, i: number) => {
        // If this Arrow buffer was written before version 4,
        // advance the buffer's bb_pos 8 bytes to skip past
        // the now-removed page_id field
        if (version < MetadataVersion.V4) {
            buffer.bb_pos += (8 * (i + 1));
        }
        return decode(buffer);
    }
}
