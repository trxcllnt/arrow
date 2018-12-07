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

import { Schema, Field } from '../../schema';
import { toUint8Array } from '../../util/buffer';
import { ArrayBufferViewInput } from '../../util/buffer';
import { MessageHeader, MetadataVersion } from '../../enum';
import { fieldFromJSON, schemaFromJSON, recordBatchFromJSON, dictionaryBatchFromJSON } from './json';

import Long = flatbuffers.Long;
import ByteBuffer = flatbuffers.ByteBuffer;
import _Int = Schema_.org.apache.arrow.flatbuf.Int;
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import _Field = Schema_.org.apache.arrow.flatbuf.Field;
import _Schema = Schema_.org.apache.arrow.flatbuf.Schema;
import _Buffer = Schema_.org.apache.arrow.flatbuf.Buffer;
import _Message = Message_.org.apache.arrow.flatbuf.Message;
import _FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;
import _RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;
import _DictionaryBatch = Message_.org.apache.arrow.flatbuf.DictionaryBatch;
import _DictionaryEncoding = Schema_.org.apache.arrow.flatbuf.DictionaryEncoding;

import {
    DataType, Dictionary, TimeBitWidth,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Map_, Struct, Union,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp,
} from '../../type';

type MessageHeaderDecoder = <T extends MessageHeader>() => T extends MessageHeader.Schema ? Schema
                                                         : T extends MessageHeader.RecordBatch ? RecordBatch
                                                         : T extends MessageHeader.DictionaryBatch ? DictionaryBatch : never;

export class Message<T extends MessageHeader = any> {

    static fromJSON<T extends MessageHeader>(msg: any, headerType: T): Message<T> {
        const message = new Message(0, MetadataVersion.V4, headerType);
        message._createHeader = messageHeaderFromJSON(msg, headerType);
        return message;
    }

    static decode(buf: ArrayBufferViewInput) {
        buf = new ByteBuffer(toUint8Array(buf));
        const _message = _Message.getRootAsMessage(buf);
        const bodyLength: Long = _message.bodyLength()!;
        const version: MetadataVersion = _message.version();
        const headerType: MessageHeader = _message.headerType();
        const message = new Message(bodyLength, version, headerType);
        message._createHeader = decodeMessageHeader(_message, headerType);
        return message;
    }

    // @ts-ignore
    public body: Uint8Array;
    public readonly headerType: T;
    public readonly bodyLength: number;
    public readonly version: MetadataVersion;
    public get type() { return this.headerType; }
    // @ts-ignore
    protected _createHeader: MessageHeaderDecoder;
    public header() { return this._createHeader<T>(); }
    public isSchema(): this is Message<MessageHeader.Schema> { return this.headerType === MessageHeader.Schema; }
    public isRecordBatch(): this is Message<MessageHeader.RecordBatch> { return this.headerType === MessageHeader.RecordBatch; }
    public isDictionaryBatch(): this is Message<MessageHeader.DictionaryBatch> { return this.headerType === MessageHeader.DictionaryBatch; }

    constructor(bodyLength: Long | number, version: MetadataVersion, headerType: T) {
        this.version = version;
        this.headerType = headerType;
        this.body = new Uint8Array(0);
        this.bodyLength = typeof bodyLength === 'number' ? bodyLength : bodyLength.low;
    }
}

export class RecordBatch {
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
    public offset: number;
    public length: number;
    constructor(offset: Long | number, length: Long | number) {
        this.offset = typeof offset === 'number' ? offset : offset.low;
        this.length = typeof length === 'number' ? length : length.low;
    }
}

export class FieldNode {
    public length: number;
    public nullCount: number;
    constructor(length: Long | number, nullCount: Long | number) {
        this.length = typeof length === 'number' ? length : length.low;
        this.nullCount = typeof nullCount === 'number' ? nullCount : nullCount.low;
    }
}

function messageHeaderFromJSON(message: any, type: MessageHeader) {
    return (() => {
        switch (type) {
            case MessageHeader.Schema: return Schema.fromJSON(message, new Map());
            case MessageHeader.RecordBatch: return RecordBatch.fromJSON(message);
            case MessageHeader.DictionaryBatch: return DictionaryBatch.fromJSON(message);
        }
        // return null;
        throw new Error(`Unrecognized Message type: { name: ${MessageHeader[type]}, type: ${type} }`);
    }) as MessageHeaderDecoder;
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

Field.decode = decodeField;
Schema.decode = decodeSchema;
RecordBatch.decode = decodeRecordBatch;
DictionaryBatch.decode = decodeDictionaryBatch;
FieldNode.decode = decodeFieldNode;
BufferRegion.decode = decodeBufferRegion;

Field.fromJSON = fieldFromJSON;
Schema.fromJSON = schemaFromJSON;
RecordBatch.fromJSON = recordBatchFromJSON;
DictionaryBatch.fromJSON = dictionaryBatchFromJSON;

declare module '../../schema' {
    namespace Field {
        export { decodeField as decode };
        export { fieldFromJSON as fromJSON };
    }
    namespace Schema {
        export { decodeSchema as decode };
        export { schemaFromJSON as fromJSON };
    }
}

declare module './message' {
    namespace RecordBatch {
        export { decodeRecordBatch as decode };
        export { recordBatchFromJSON as fromJSON };
    }
    namespace DictionaryBatch {
        export { decodeDictionaryBatch as decode };
        export { dictionaryBatchFromJSON as fromJSON };
    }
    namespace FieldNode { export { decodeFieldNode as decode }; }
    namespace BufferRegion { export { decodeBufferRegion as decode }; }
}

function decodeSchema(_schema: _Schema, dictionaryTypes: Map<number, Dictionary>) {
    const fields = decodeSchemaFields(_schema, dictionaryTypes);
    return new Schema(fields, decodeCustomMetadata(_schema), dictionaryTypes);
}

function decodeRecordBatch(batch: _RecordBatch, version = MetadataVersion.V4) {
    return new RecordBatch(batch.length(), decodeFieldNodes(batch), decodeBuffers(batch, version));
}

function decodeDictionaryBatch(batch: _DictionaryBatch, version = MetadataVersion.V4) {
    return new DictionaryBatch(RecordBatch.decode(batch.data()!, version), batch.id(), batch.isDelta());
}

function decodeBufferRegion(b: _Buffer) {
    return new BufferRegion(b.offset(), b.length());
}

function decodeFieldNode(f: _FieldNode) {
    return new FieldNode(f.length(), f.nullCount());
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
    };
}

function decodeSchemaFields(schema: _Schema, dictionaryTypes: Map<number, Dictionary> | null) {
    return Array.from(
        { length: schema.fieldsLength() },
        (_, i) => schema.fields(i)!
    ).filter(Boolean).map((f) => Field.decode(f, dictionaryTypes));
}

function decodeFieldChildren(field: _Field, dictionaryTypes: Map<number, Dictionary> | null): Field[] {
    return Array.from(
        { length: field.childrenLength() },
        (_, i) => field.children(i)!
    ).filter(Boolean).map((f) => Field.decode(f, dictionaryTypes));
}

function decodeField(f: _Field, dictionaryTypes: Map<number, Dictionary> | null) {

    let id: number;
    let field: Field | void;
    let keys: _Int | Int | null;
    let type: DataType<any> | null;
    let dict: _DictionaryEncoding | null;

    // If no dictionary encoding, or in the process of decoding the children of a dictionary-encoded field
    if (!dictionaryTypes || !(dict = f.dictionary())) {
        type = decodeFieldType(f, decodeFieldChildren(f, dictionaryTypes));
        field = new Field(f.name()!, type, f.nullable(), decodeCustomMetadata(f));
    }
    // tslint:disable
    // If dictionary encoded and the first time we've seen this dictionary id, decode
    // the data type and child fields, then wrap in a Dictionary type and insert the
    // data type into the dictionary types map.
    else if (!dictionaryTypes.has(id = dict.id().low)) {
        // a dictionary index defaults to signed 32 bit int if unspecified
        keys = (keys = dict.indexType()) ? decodeIntType(keys) : new Int(true, 32);
        type = new Dictionary(decodeFieldType(f, decodeFieldChildren(f, null)), keys, id, dict.isOrdered());
        field = new Field(f.name()!, type, f.nullable(), decodeCustomMetadata(f));
        dictionaryTypes.set(id, type as Dictionary);
    }
    // If dictionary encoded, and have already seen this dictionary Id in the schema, then reuse the
    // data type and wrap in a new Dictionary type and field.
    else {
        // a dictionary index defaults to signed 32 bit int if unspecified
        keys = (keys = dict.indexType()) ? decodeIntType(keys) : new Int(true, 32);
        field = new Field(f.name()!, dictionaryTypes.get(id)!, f.nullable(), decodeCustomMetadata(f));
    }
    return field || null;
}

function decodeCustomMetadata(parent?: _Schema | _Field | null) {
    const data = new Map<string, string>();
    if (parent) {
        for (let entry, key, i = -1, n = parent.customMetadataLength() | 0; ++i < n;) {
            if ((entry = parent.customMetadata(i)) && (key = entry.key()) != null) {
                data.set(key, entry.value()!);
            }
        }
    }
    return data;
}

function decodeIntType(_type: _Int) {
    return new Int(_type.isSigned(), _type.bitWidth());
}

function decodeFieldType(f: _Field, children?: Field[]): DataType<any> {

    const typeId = f.typeType();

    switch (typeId) {
        case Type.NONE:    return new DataType();
        case Type.Null:    return new Null();
        case Type.Binary:  return new Binary();
        case Type.Utf8:    return new Utf8();
        case Type.Bool:    return new Bool();
        case Type.List:    return new List(children || []);
        case Type.Struct_: return new Struct(children || []);
    }

    switch (typeId) {
        case Type.Int: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Int())!;
            return new Int(t.isSigned(), t.bitWidth());
        }
        case Type.FloatingPoint: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.FloatingPoint())!;
            return new Float(t.precision());
        }
        case Type.Decimal: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Decimal())!;
            return new Decimal(t.scale(), t.precision());
        }
        case Type.Date: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Date())!;
            return new Date_(t.unit());
        }
        case Type.Time: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Time())!;
            return new Time(t.unit(), t.bitWidth() as TimeBitWidth);
        }
        case Type.Timestamp: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Timestamp())!;
            return new Timestamp(t.unit(), t.timezone());
        }
        case Type.Interval: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Interval())!;
            return new Interval(t.unit());
        }
        case Type.Union: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Union())!;
            return new Union(t.mode(), (t.typeIdsArray() || []) as Type[], children || []);
        }
        case Type.FixedSizeBinary: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.FixedSizeBinary())!;
            return new FixedSizeBinary(t.byteWidth());
        }
        case Type.FixedSizeList: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.FixedSizeList())!;
            return new FixedSizeList(t.listSize(), children || []);
        }
        case Type.Map: {
            const t = f.type(new Schema_.org.apache.arrow.flatbuf.Map())!;
            return new Map_(children || [], t.keysSorted());
        }
    }
    throw new Error(`Unrecognized type: "${Type[typeId]}" (${typeId})`);
}
