import { TypedArray } from '../vector/types';
import { ArrowType, ArrowTypeVisitor } from './visitor';
import { flatbuffers } from 'flatbuffers';
import Long = flatbuffers.Long;
import Builder = flatbuffers.Builder;
import * as Schema_ from '../format/Schema';
import * as Message_ from '../format/Message';

import Null = Schema_.org.apache.arrow.flatbuf.Null;
import Int = Schema_.org.apache.arrow.flatbuf.Int;
import FloatingPoint = Schema_.org.apache.arrow.flatbuf.FloatingPoint;
import Binary = Schema_.org.apache.arrow.flatbuf.Binary;
import Bool = Schema_.org.apache.arrow.flatbuf.Bool;
import Utf8 = Schema_.org.apache.arrow.flatbuf.Utf8;
import Decimal = Schema_.org.apache.arrow.flatbuf.Decimal;
import Date = Schema_.org.apache.arrow.flatbuf.Date;
import Time = Schema_.org.apache.arrow.flatbuf.Time;
import Timestamp = Schema_.org.apache.arrow.flatbuf.Timestamp;
import Interval = Schema_.org.apache.arrow.flatbuf.Interval;
import List = Schema_.org.apache.arrow.flatbuf.List;
import Struct = Schema_.org.apache.arrow.flatbuf.Struct_;
import Union = Schema_.org.apache.arrow.flatbuf.Union;
import FixedSizeBinary = Schema_.org.apache.arrow.flatbuf.FixedSizeBinary;
import FixedSizeList = Schema_.org.apache.arrow.flatbuf.FixedSizeList;
import Map = Schema_.org.apache.arrow.flatbuf.Map;

import Field = Schema_.org.apache.arrow.flatbuf.Field;
import Schema = Schema_.org.apache.arrow.flatbuf.Schema;
import Buffer = Schema_.org.apache.arrow.flatbuf.Buffer;
import Message = Message_.org.apache.arrow.flatbuf.Message;
import KeyValue = Schema_.org.apache.arrow.flatbuf.KeyValue;
import FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;
import RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;
import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;
import DictionaryBatch = Message_.org.apache.arrow.flatbuf.DictionaryBatch;
import DictionaryEncoding = Schema_.org.apache.arrow.flatbuf.DictionaryEncoding;

import {
    ArrowBuffer,
    ArrowField,
    ArrowSchema,
    ArrowFieldNode,
    ArrowMessage,
    ArrowRecordBatch,
    ArrowDictionaryBatch,
    ArrowDictionaryEncoding,
} from './message';

import {
    NullFieldType,
    IntFieldType,
    FloatingPointFieldType,
    BinaryFieldType,
    BoolFieldType,
    Utf8FieldType,
    DecimalFieldType,
    DateFieldType,
    TimeFieldType,
    TimestampFieldType,
    IntervalFieldType,
    ListFieldType,
    StructFieldType,
    UnionFieldType,
    FixedSizeBinaryFieldType,
    FixedSizeListFieldType,
    MapFieldType,
} from './field';

const addInt8  = (b: Builder, x: number) => b.addInt8(x);
const addInt16 = (b: Builder, x: number) => b.addInt16(x);
const addInt32 = (b: Builder, x: number) => b.addInt32(x);
const addFloat32 = (b: Builder, x: number) => b.addFloat32(x);
const addFloat64 = (b: Builder, x: number) => b.addFloat64(x);
const extra = (x: number, to: number) => to - (x % to);
const align = (x: number, to: number) => (x % to) === 0 ? x : x + extra(x, to);

export class BufferWriter extends ArrowTypeVisitor {
    static writeMessage(message: ArrowMessage, builder?: Builder) {
        let b = builder || new Builder(), start = b.offset();
        let messageHeaderOffset = new BufferWriter(b).visit(message);
        Message.finishMessageBuffer(b,
            Message.startMessage(b) ||
            Message.addVersion(b, MetadataVersion.V4) ||
            Message.addHeader(b, messageHeaderOffset) ||
            Message.addHeaderType(b, message.getHeaderType()) ||
            Message.addBodyLength(b, message.computeBodyLength()) ||
            Message.endMessage(b)
        );
        let bb = b.dataBuffer();
        // calculate alignment bytes so that metadata length points to the correct location after alignment
        // reinvoke `dataBuffer()` because the underlying ByteBuffer may have been recreated by adding data,
        // and we need to pad the position of the current ByteBuffer.
        bb = b.addInt32(align(b.offset() - start + 4, 8)) || b.dataBuffer();
        bb = b.pad(extra(bb.position(), 8)) || b.dataBuffer();
        return bb;
    }
    static writeBuffer(data: TypedArray, builder?: Builder) {
        const b = builder || new Builder(), start = b.offset();
        const write = data instanceof Float32Array ? addFloat32 :
                      data instanceof Float64Array ? addFloat64 :
                      data.BYTES_PER_ELEMENT === 4 ? addInt32 :
                      data.BYTES_PER_ELEMENT === 2 ? addInt16 : addInt8;
        for (let i = data.length; ~(--i);) {
            write(b, data[i]);
        }
        const size = b.offset() - start;
        const offset = new Long(start, 0);
        const length = new Long(align(size, 8), 0);
        // pad position to the nearest 8 bytes
        b.pad(extra(size, 8));
        return { builder: b, offset, length };
    }
    constructor(protected builder: Builder) {
        super();
    }
    visit(node: ArrowType) {
        return super.visit(node) as number;
    }
    visitMany(nodes: ArrowType[]) {
        return super.visitMany(nodes) as number[];
    }
    visitSchema(node: ArrowSchema) {
        const b = this.builder;
        const fieldOffsets = this.visitMany(node.fields);
        const fieldsOffset = Schema.startFieldsVector(b, fieldOffsets.length) ||
                             Schema.createFieldsVector(b, fieldOffsets);
        return Schema.startSchema(b) ||
               Schema.addFields(b, fieldsOffset) ||
               Schema.addEndianness(b, node.endianness) ||
               Schema.endSchema(b);
    }
    visitRecordBatch(node: ArrowRecordBatch) {
        const b = this.builder;
        let nodesOffset: number | void = undefined;
        let buffersOffset: number | void = undefined;
        if (node.nodes && node.nodes.length) {
            RecordBatch.startNodesVector(b, node.nodes.length);
            nodesOffset = this.visitMany(node.nodes) && b.endVector();
        }
        if (node.buffers && node.buffers.length) {
            RecordBatch.startBuffersVector(b, node.buffers.length);
            buffersOffset = this.visitMany(node.buffers) && b.endVector();
        }
        return RecordBatch.startRecordBatch(b) ||
               RecordBatch.addLength(b, node.length) ||
               (nodesOffset !== undefined && RecordBatch.addNodes(b, nodesOffset)) ||
               (buffersOffset !== undefined && RecordBatch.addBuffers(b, buffersOffset)) ||
               RecordBatch.endRecordBatch(b);
    }
    visitDictionaryBatch(node: ArrowDictionaryBatch) {
        const b = this.builder;
        const dataOffset = this.visit(node.dictionary);
        return DictionaryBatch.startDictionaryBatch(b) ||
               DictionaryBatch.addId(b, node.dictionaryId) ||
               DictionaryBatch.addIsDelta(b, node.isDelta) ||
               DictionaryBatch.addData(b, dataOffset) ||
               DictionaryBatch.endDictionaryBatch(b);
    }
    visitBuffer(node: ArrowBuffer) {
        return Buffer.createBuffer(this.builder, node.offset, node.length);
    }
    visitFieldNode(node: ArrowFieldNode) {
        return FieldNode.createFieldNode(this.builder, node.length, node.null_count);
    }
    visitField(node: ArrowField): number {
        const b = this.builder;
        let type: number = this.visit(node.type);
        let name: number = b.createString(node.name || '');
        let children: number | void = undefined;
        let metadata: number | void = undefined;
        let dictionary: number | void = undefined;
        if (node.children && node.children.length) {
            children = Field.startChildrenVector(b, node.children.length) ||
                       Field.createChildrenVector(b, this.visitMany(node.children));
        }
        if (node.dictionary) {
            node.dictionary.indexType = type;
            dictionary = this.visit(node.dictionary);
        }
        if (node.metadata) {
            const keyvals = [];
            for (const [k, v] of Object.entries(node.metadata)) {
                const key = b.createString(k);
                const val = b.createString(v);
                keyvals.push(
                    KeyValue.startKeyValue(b) ||
                    KeyValue.addKey(b, key) ||
                    KeyValue.addValue(b, val) ||
                    KeyValue.endKeyValue(b)
                );
            }
            metadata = Field.createCustomMetadataVector(b, keyvals);
        }
        return Field.startField(b) ||
               Field.addName(b, name) ||
               Field.addType(b, type) ||
               Field.addTypeType(b, node.typeType) ||
               Field.addNullable(b, !!node.nullable) ||
               (children !== undefined && Field.addChildren(b, children)) ||
               (dictionary !== undefined && Field.addDictionary(b, dictionary)) ||
               (metadata !== undefined && Field.addCustomMetadata(b, metadata)) ||
               Field.endField(b);
    }
    visitDictionaryEncoding(node: ArrowDictionaryEncoding) {
        const b = this.builder;
        const indexType = node.indexType !== undefined ? this.visit(node.indexType) : undefined;
        return DictionaryEncoding.startDictionaryEncoding(b) ||
               DictionaryEncoding.addId(b, node.dictionaryId) ||
               DictionaryEncoding.addIsOrdered(b, node.isOrdered) ||
               (indexType !== undefined && DictionaryEncoding.addIndexType(b, indexType)) ||
               DictionaryEncoding.endDictionaryEncoding(b);
    }
    visitNullFieldType(node: NullFieldType) {
        const b = this.builder;
        return Null.startNull(b) ||
               Null.endNull(b);
    }
    visitIntFieldType(node: IntFieldType) {
        const b = this.builder;
        return Int.startInt(b) ||
               Int.addBitWidth(b, node.bitWidth) ||
               Int.addIsSigned(b, node.isSigned) ||
               Int.endInt(b);
    }
    visitFloatingPointFieldType(node: FloatingPointFieldType) {
        const b = this.builder;
        return FloatingPoint.startFloatingPoint(b) ||
               FloatingPoint.addPrecision(b, node.precision) ||
               FloatingPoint.endFloatingPoint(b);
    }
    visitBinaryFieldType(_node: BinaryFieldType) {
        const b = this.builder;
        return Binary.startBinary(b) ||
               Binary.endBinary(b);
    }
    visitBoolFieldType(_node: BoolFieldType) {
        const b = this.builder;
        return Bool.startBool(b) ||
               Bool.endBool(b);
    }
    visitUtf8FieldType(_node: Utf8FieldType) {
        const b = this.builder;
        return Utf8.startUtf8(b) ||
               Utf8.endUtf8(b);
    }
    visitDecimalFieldType(node: DecimalFieldType) {
        const b = this.builder;
        return Decimal.startDecimal(b) ||
               Decimal.addScale(b, node.scale) ||
               Decimal.addPrecision(b, node.precision) ||
               Decimal.endDecimal(b);
    }
    visitDateFieldType(node: DateFieldType) {
        const b = this.builder;
        return Date.startDate(b) ||
               Date.addUnit(b, node.unit) ||
               Date.endDate(b);
    }
    visitTimeFieldType(node: TimeFieldType) {
        const b = this.builder;
        return Time.startTime(b) ||
               Time.addUnit(b, node.unit) ||
               Time.addBitWidth(b, node.bitWidth) ||
               Time.endTime(b);
    }
    visitTimestampFieldType(node: TimestampFieldType) {
        const b = this.builder;
        const timezone = node.timezone && b.createString(node.timezone) || undefined;
        return Timestamp.startTimestamp(b) ||
               Timestamp.addUnit(b, node.unit) ||
               (timezone !== undefined && Timestamp.addTimezone(b, timezone)) ||
               Timestamp.endTimestamp(b);
    }
    visitIntervalFieldType(node: IntervalFieldType) {
        const b = this.builder;
        return Interval.startInterval(b) ||
               Interval.addUnit(b, node.unit) ||
               Interval.endInterval(b);
    }
    visitListFieldType(_node: ListFieldType) {
        const b = this.builder;
        return List.startList(b) ||
               List.endList(b);
    }
    visitStructFieldType(_node: StructFieldType) {
        const b = this.builder;
        return Struct.startStruct_(b) ||
               Struct.endStruct_(b);
    }
    visitUnionFieldType(node: UnionFieldType) {
        const b = this.builder;
        const typeIds = 
               Union.startTypeIdsVector(b, node.typeIds.length) ||
               Union.createTypeIdsVector(b, node.typeIds);
        return Union.startUnion(b) ||
               Union.addMode(b, node.mode) ||
               Union.addTypeIds(b, typeIds) ||
               Union.endUnion(b);
    }
    visitFixedSizeBinaryFieldType(node: FixedSizeBinaryFieldType) {
        const b = this.builder;
        return FixedSizeBinary.startFixedSizeBinary(b) ||
               FixedSizeBinary.addByteWidth(b, node.byteWidth) ||
               FixedSizeBinary.endFixedSizeBinary(b);
    }
    visitFixedSizeListFieldType(node: FixedSizeListFieldType) {
        const b = this.builder;
        return FixedSizeList.startFixedSizeList(b) ||
               FixedSizeList.addListSize(b, node.listSize) ||
               FixedSizeList.endFixedSizeList(b);
    }
    visitMapFieldType(node: MapFieldType) {
        const b = this.builder;
        return Map.startMap(b) ||
               Map.addKeysSorted(b, node.keysSorted) ||
               Map.endMap(b);
    }
}
