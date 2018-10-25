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

    TimeBitWidth, Int32,
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
import _Null = Schema_.org.apache.arrow.flatbuf.Null;
import _Int = Schema_.org.apache.arrow.flatbuf.Int;
import _FloatingPoint = Schema_.org.apache.arrow.flatbuf.FloatingPoint;
import _Binary = Schema_.org.apache.arrow.flatbuf.Binary;
import _Bool = Schema_.org.apache.arrow.flatbuf.Bool;
import _Utf8 = Schema_.org.apache.arrow.flatbuf.Utf8;
import _Decimal = Schema_.org.apache.arrow.flatbuf.Decimal;
import _Date = Schema_.org.apache.arrow.flatbuf.Date;
import _Time = Schema_.org.apache.arrow.flatbuf.Time;
import _Timestamp = Schema_.org.apache.arrow.flatbuf.Timestamp;
import _Interval = Schema_.org.apache.arrow.flatbuf.Interval;
import _List = Schema_.org.apache.arrow.flatbuf.List;
import _Struct = Schema_.org.apache.arrow.flatbuf.Struct_;
import _Union = Schema_.org.apache.arrow.flatbuf.Union;
import _FixedSizeBinary = Schema_.org.apache.arrow.flatbuf.FixedSizeBinary;
import _FixedSizeList = Schema_.org.apache.arrow.flatbuf.FixedSizeList;
import _Map = Schema_.org.apache.arrow.flatbuf.Map;

// function footerFromByteBuffer(bb: ByteBuffer) {
//     const dictionaryFields = new Map<number, Field<Dictionary>>();
//     const f = _Footer.getRootAsFooter(bb), s = f.schema()!;
//     return new Footer(
//         dictionaryBatchesFromFooter(f), recordBatchesFromFooter(f),
//         new Schema(fieldsFromSchema(s, dictionaryFields), customMetadata(s), f.version(), dictionaryFields)
//     );
// }

export function schemaFromMessage(message: Message, dictionaryFields: Map<number, Field<Dictionary>>) {
    const v: MetadataVersion = message.version, s: _Schema = message.header(new _Schema())!;
    return new Schema(fieldsFromSchema(s, dictionaryFields), customMetadata(s), v, dictionaryFields);
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

function fieldsFromSchema(s: _Schema, dictionaryFields: Map<number, Field<Dictionary>> | null) {
    const fields = [] as Field[];
    for (let i = -1, c: Field | null, n = s && s.fieldsLength(); ++i < n;) {
        if (c = field(s.fields(i)!, dictionaryFields)) {
            fields.push(c);
        }
    }
    return fields;
}

function fieldsFromField(f: _Field, dictionaryFields: Map<number, Field<Dictionary>> | null) {
    const fields = [] as Field[];
    for (let i = -1, c: Field | null, n = f && f.childrenLength(); ++i < n;) {
        if (c = field(f.children(i)!, dictionaryFields)) {
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

function field(f: _Field, dictionaryFields: Map<number, Field<Dictionary>> | null) {
    let name = f.name()!;
    let field: Field | void;
    let nullable = f.nullable();
    let metadata = customMetadata(f);
    let dataType: DataType<any> | null;
    let keysMeta: _Int | null, id: number;
    let dictMeta: _DictionaryEncoding | null;
    if (!dictionaryFields || !(dictMeta = f.dictionary())) {
        if (dataType = typeFromField(f, fieldsFromField(f, dictionaryFields))) {
            field = new Field(name, dataType, nullable, metadata);
        }
    } else if (dataType = dictionaryFields.has(id = dictMeta.id().low)
                        ? dictionaryFields.get(id)!.type.dictionary
                        : typeFromField(f, fieldsFromField(f, null))) {
        dataType = new Dictionary(dataType,
            // a dictionary index defaults to signed 32 bit int if unspecified
            (keysMeta = dictMeta.indexType()) ? intFromField(keysMeta)! : new Int32(),
            id, dictMeta.isOrdered()
        );
        field = new Field(name, dataType, nullable, metadata);
        dictionaryFields.has(id) || dictionaryFields.set(id, field as Field<Dictionary>);
    }
    return field || null;
}

function customMetadata(parent?: _Schema | _Field | null) {
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

function typeFromField(f: _Field, children?: Field[]): DataType<any> | null {
    switch (f.typeType()) {
        case Type.NONE: return null;
        case Type.Null: return nullFromField(f.type(new _Null())!);
        case Type.Int: return intFromField(f.type(new _Int())!);
        case Type.FloatingPoint: return floatFromField(f.type(new _FloatingPoint())!);
        case Type.Binary: return binaryFromField(f.type(new _Binary())!);
        case Type.Utf8: return utf8FromField(f.type(new _Utf8())!);
        case Type.Bool: return boolFromField(f.type(new _Bool())!);
        case Type.Decimal: return decimalFromField(f.type(new _Decimal())!);
        case Type.Date: return dateFromField(f.type(new _Date())!);
        case Type.Time: return timeFromField(f.type(new _Time())!);
        case Type.Timestamp: return timestampFromField(f.type(new _Timestamp())!);
        case Type.Interval: return intervalFromField(f.type(new _Interval())!);
        case Type.List: return listFromField(f.type(new _List())!, children || []);
        case Type.Struct_: return structFromField(f.type(new _Struct())!, children || []);
        case Type.Union: return unionFromField(f.type(new _Union())!, children || []);
        case Type.FixedSizeBinary: return fixedSizeBinaryFromField(f.type(new _FixedSizeBinary())!);
        case Type.FixedSizeList: return fixedSizeListFromField(f.type(new _FixedSizeList())!, children || []);
        case Type.Map: return mapFromField(f.type(new _Map())!, children || []);
    }
    throw new Error(`Unrecognized type ${f.typeType()}`);
}

function nullFromField           (_type: _Null)                             { return new Null();                                                                }
function intFromField            (_type: _Int)                              { return new Int(_type.isSigned(), _type.bitWidth());                               }
function floatFromField          (_type: _FloatingPoint)                    { return new Float(_type.precision());                                              }
function binaryFromField         (_type: _Binary)                           { return new Binary();                                                              }
function utf8FromField           (_type: _Utf8)                             { return new Utf8();                                                                }
function boolFromField           (_type: _Bool)                             { return new Bool();                                                                }
function decimalFromField        (_type: _Decimal)                          { return new Decimal(_type.scale(), _type.precision());                             }
function dateFromField           (_type: _Date)                             { return new Date_(_type.unit());                                                   }
function timeFromField           (_type: _Time)                             { return new Time(_type.unit(), _type.bitWidth() as TimeBitWidth);                  }
function timestampFromField      (_type: _Timestamp)                        { return new Timestamp(_type.unit(), _type.timezone());                             }
function intervalFromField       (_type: _Interval)                         { return new Interval(_type.unit());                                                }
function listFromField           (_type: _List, children: Field[])          { return new List(children);                                                        }
function structFromField         (_type: _Struct, children: Field[])        { return new Struct(children);                                                      }
function unionFromField          (_type: _Union, children: Field[])         { return new Union(_type.mode(), (_type.typeIdsArray() || []) as Type[], children); }
function fixedSizeBinaryFromField(_type: _FixedSizeBinary)                  { return new FixedSizeBinary(_type.byteWidth());                                    }
function fixedSizeListFromField  (_type: _FixedSizeList, children: Field[]) { return new FixedSizeList(_type.listSize(), children);                             }
function mapFromField            (_type: _Map, children: Field[])           { return new Map_(children, _type.keysSorted());                                    }
