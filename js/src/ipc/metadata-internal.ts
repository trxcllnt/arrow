// import * as File_ from '../fb/File';
import * as Schema_ from '../fb/Schema';
// import * as Message_ from '../fb/Message';
// import { flatbuffers } from 'flatbuffers';

import { Message } from './message';
import { Schema, Field } from '../schema';
// import ByteBuffer = flatbuffers.ByteBuffer;
import {
    DataType, Dictionary,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Map_, Struct, Union,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp,

    TimeBitWidth,
    // DenseUnion, SparseUnion,
    // DateDay, DateMillisecond,
    // IntervalDayTime, IntervalYearMonth,
    // TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    // TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    // Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Float16, Float32, Float64,
} from '../type';

import Type = Schema_.org.apache.arrow.flatbuf.Type;
// import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
// import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;
import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;
// import _Footer = File_.org.apache.arrow.flatbuf.Footer;
// import _Block = File_.org.apache.arrow.flatbuf.Block;
// import _Message = Message_.org.apache.arrow.flatbuf.Message;
import _Schema = Schema_.org.apache.arrow.flatbuf.Schema;
import _Field = Schema_.org.apache.arrow.flatbuf.Field;
// import _RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;
// import _DictionaryBatch = Message_.org.apache.arrow.flatbuf.DictionaryBatch;
// import _FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;
// import _Buffer = Schema_.org.apache.arrow.flatbuf.Buffer;
import _DictionaryEncoding = Schema_.org.apache.arrow.flatbuf.DictionaryEncoding;
import _Int = Schema_.org.apache.arrow.flatbuf.Int;
import _FloatingPoint = Schema_.org.apache.arrow.flatbuf.FloatingPoint;
import _Decimal = Schema_.org.apache.arrow.flatbuf.Decimal;
import _Date = Schema_.org.apache.arrow.flatbuf.Date;
import _Time = Schema_.org.apache.arrow.flatbuf.Time;
import _Timestamp = Schema_.org.apache.arrow.flatbuf.Timestamp;
import _Interval = Schema_.org.apache.arrow.flatbuf.Interval;
import _Union = Schema_.org.apache.arrow.flatbuf.Union;
import _FixedSizeBinary = Schema_.org.apache.arrow.flatbuf.FixedSizeBinary;
import _FixedSizeList = Schema_.org.apache.arrow.flatbuf.FixedSizeList;
import _Map = Schema_.org.apache.arrow.flatbuf.Map;

// function footerFromByteBuffer(bb: ByteBuffer) {
//     const dictionaryFields = new Map<number, DataType>();
//     const f = _Footer.getRootAsFooter(bb), s = f.schema()!;
//     return new Footer(
//         dictionaryBatchesFromFooter(f), recordBatchesFromFooter(f),
//         new Schema(fieldsFromSchema(s, dictionaryFields), customMetadata(s), f.version(), dictionaryFields)
//     );
// }

export function getSchema(message: Message, dictionaryTypes: Map<number, DataType>) {
    const v: MetadataVersion = message.version, s: _Schema = message.header(new _Schema())!;
    return new Schema(getFields(s, dictionaryTypes), getCustomMetadata(s), v, dictionaryTypes);
}

// function recordBatchFromMessage(version: MetadataVersion, m: _Message, b: _RecordBatch) {
//     return new RecordBatchMetadata(version, b.length(), fieldNodesFromRecordBatch(b), buffersFromRecordBatch(b, version), m.bodyLength());
// }

// function dictionaryBatchFromMessage(version: MetadataVersion, m: _Message, d: _DictionaryBatch) {
//     return new DictionaryBatch(version, recordBatchFromMessage(version, m, d.data()!), d.id(), d.isDelta());
// }

// function dictionaryBatchesFromFooter(f: _Footer) {
//     const blocks = [] as FileBlock[];
//     for (let b: _Block, i = -1, n = f && f.dictionariesLength(); ++i < n;) {
//         if (b = f.dictionaries(i)!) {
//             blocks.push(new FileBlock(b.metaDataLength(), b.bodyLength(), b.offset()));
//         }
//     }
//     return blocks;
// }

// function recordBatchesFromFooter(f: _Footer) {
//     const blocks = [] as FileBlock[];
//     for (let b: _Block, i = -1, n = f && f.recordBatchesLength(); ++i < n;) {
//         if (b = f.recordBatches(i)!) {
//             blocks.push(new FileBlock(b.metaDataLength(), b.bodyLength(), b.offset()));
//         }
//     }
//     return blocks;
// }

function getFields(s: _Schema, dictionaryTypes: Map<number, DataType> | null) {
    const fields = [] as Field[];
    for (let i = -1, c: Field | null, n = s && s.fieldsLength(); ++i < n;) {
        if (c = getField(s.fields(i)!, dictionaryTypes)) {
            fields.push(c);
        }
    }
    return fields;
}

function getChildFields(f: _Field, dictionaryTypes: Map<number, DataType> | null) {
    const fields = [] as Field[];
    for (let i = -1, c: Field | null, n = f && f.childrenLength(); ++i < n;) {
        if (c = getField(f.children(i)!, dictionaryTypes)) {
            fields.push(c);
        }
    }
    return fields;
}

// function fieldNodesFromRecordBatch(b: _RecordBatch) {
//     const fieldNodes = [] as FieldMetadata[];
//     for (let i = -1, n = b.nodesLength(); ++i < n;) {
//         fieldNodes.push(fieldNodeFromRecordBatch(b.nodes(i)!));
//     }
//     return fieldNodes;
// }

// function buffersFromRecordBatch(b: _RecordBatch, version: MetadataVersion) {
//     const buffers = [] as BufferMetadata[];
//     for (let i = -1, n = b.buffersLength(); ++i < n;) {
//         let buffer = b.buffers(i)!;
//         // If this Arrow buffer was written before version 4,
//         // advance the buffer's bb_pos 8 bytes to skip past
//         // the now-removed page id field.
//         if (version < MetadataVersion.V4) {
//             buffer.bb_pos += (8 * (i + 1));
//         }
//         buffers.push(bufferFromRecordBatch(buffer));
//     }
//     return buffers;
// }

function getField(f: _Field, dictionaryTypes: Map<number, DataType> | null) {
    let id: number;
    let field: Field | void;
    let keys: _Int | Int | null;
    let type: DataType<any> | null;
    let dict: _DictionaryEncoding | null;
    // If no dictionary encoding, or in the process of decoding the children of a dictionary-encoded field
    if (!dictionaryTypes || !(dict = f.dictionary())) {
        type = getType(f, getChildFields(f, dictionaryTypes));
        field = new Field(f.name()!, type, f.nullable(), getCustomMetadata(f));
    }
    // tslint:disable
    // If dictionary encoded and the first time we've seen this dictionary id, decode
    // the data type and child fields, then wrap in a Dictionary type and insert the
    // data type into the dictionary types map.
    else if (!dictionaryTypes.has(id = dict.id().low)) {
        // a dictionary index defaults to signed 32 bit int if unspecified
        keys = (keys = dict.indexType()) ? getIntType(keys) : new Int(true, 32);
        type = new Dictionary(getType(f, getChildFields(f, null)), keys, id, dict.isOrdered());
        field = new Field(f.name()!, type, f.nullable(), getCustomMetadata(f));
        dictionaryTypes.set(id, (type as Dictionary).dictionary);
    }
    // If dictionary encoded, and have already seen this dictionary Id in the schema, then reuse the
    // data type and wrap in a new Dictionary type and field.
    else {
        // a dictionary index defaults to signed 32 bit int if unspecified
        keys = (keys = dict.indexType()) ? getIntType(keys) : new Int(true, 32);
        type = new Dictionary(dictionaryTypes.get(id)!, keys, id, dict.isOrdered());
        field = new Field(f.name()!, type, f.nullable(), getCustomMetadata(f));
    }
    return field || null;
}

function getCustomMetadata(parent?: _Schema | _Field | null) {
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

// function fieldNodeFromRecordBatch(f: _FieldNode) {
//     return new FieldMetadata(f.length(), f.nullCount());
// }

// function bufferFromRecordBatch(b: _Buffer) {
//     return new BufferMetadata(b.offset(), b.length());
// }

function getType(f: _Field, children?: Field[]): DataType<any> {

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
            const t = f.type(new _Int())!;
            return new Int(t.isSigned(), t.bitWidth());
        }
        case Type.FloatingPoint: {
            const t = f.type(new _FloatingPoint())!;
            return new Float(t.precision());
        }
        case Type.Decimal: {
            const t = f.type(new _Decimal())!;
            return new Decimal(t.scale(), t.precision());
        }
        case Type.Date: {
            const t = f.type(new _Date())!;
            return new Date_(t.unit());
        }
        case Type.Time: {
            const t = f.type(new _Time())!;
            return new Time(t.unit(), t.bitWidth() as TimeBitWidth);
        }
        case Type.Timestamp: {
            const t = f.type(new _Timestamp())!;
            return new Timestamp(t.unit(), t.timezone());
        }
        case Type.Interval: {
            const t = f.type(new _Interval())!;
            return new Interval(t.unit());
        }
        case Type.Union: {
            const t = f.type(new _Union())!;
            return new Union(t.mode(), (t.typeIdsArray() || []) as Type[], children || []);
        }
        case Type.FixedSizeBinary: {
            const t = f.type(new _FixedSizeBinary())!;
            return new FixedSizeBinary(t.byteWidth());
        }
        case Type.FixedSizeList: {
            const t = f.type(new _FixedSizeList())!;
            return new FixedSizeList(t.listSize(), children || []);
        }
        case Type.Map: {
            const t = f.type(new _Map())!;
            return new Map_(children || [], t.keysSorted());
        }
    }
    throw new Error(`Unrecognized type: "${Type[typeId]}" (${typeId})`);
}

function getIntType (_type: _Int) {
    return new Int(_type.isSigned(), _type.bitWidth());
}
