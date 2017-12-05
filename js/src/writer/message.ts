import { FieldType } from './field';
import { flatbuffers } from 'flatbuffers';
import { ArrowType, ArrowTypeVisitor } from './visitor';
import * as Schema_ from '../format/Schema';
import * as Message_ from '../format/Message';
import Long = flatbuffers.Long;
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import Endianness = Schema_.org.apache.arrow.flatbuf.Endianness;
import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;

const extra = (x: number, to: number) => to - (x % to);
const align = (x: number, to: number) => (x % to) === 0 ? x : x + extra(x, to);

export type ArrowMessage = ArrowSchema | ArrowRecordBatch | ArrowDictionaryBatch;

export class ArrowSchema extends ArrowType {
    static headerType = MessageHeader.Schema;
    static create(fields: ArrowField[], customMetadata?: Map<string, string>, endianness = Endianness.Little) {
        return new ArrowSchema(fields, customMetadata, endianness);
    }
    constructor(public fields: ArrowField[], public customMetadata?: Map<string, string>, public endianness = Endianness.Little) {
        super();
    }
    getHeaderType() { return ArrowSchema.headerType; }
    computeBodyLength() { return Long.ZERO; }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitSchema(this);
    }
}

export class ArrowRecordBatch extends ArrowType {
    static headerType = MessageHeader.RecordBatch;
    static create(length: Long, nodes: ArrowFieldNode[], buffers: ArrowBuffer[]) {
        return new ArrowRecordBatch(length, nodes, buffers);
    }
    constructor(public length: Long, public nodes: ArrowFieldNode[], public buffers: ArrowBuffer[]) {
        super();
    }
    getHeaderType() { return ArrowRecordBatch.headerType; }
    computeBodyLength(): Long {
        let size = 0, buffers = this.buffers;
        for (let i = -1, n = buffers.length; ++i < n;) {
            size = align(size += (buffers[i].offset.low - size), 8);
        }
        return new Long(size, 0);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitRecordBatch(this);
    }
}

export class ArrowDictionaryBatch extends ArrowType {
    static atomicDictionaryId = 0;
    static headerType = MessageHeader.DictionaryBatch;
    static create(recordBatch: ArrowRecordBatch, dictionaryId = new Long(ArrowDictionaryBatch.atomicDictionaryId++, 0), isDelta = false) {
        return new ArrowDictionaryBatch(recordBatch, dictionaryId, isDelta);
    }
    constructor(public dictionary: ArrowRecordBatch, public dictionaryId: Long, public isDelta: boolean) {
        super();
    }
    getHeaderType() { return ArrowDictionaryBatch.headerType; }
    computeBodyLength(): Long { return this.dictionary.computeBodyLength(); }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitDictionaryBatch(this);
    }
}

export class ArrowBuffer extends ArrowType {
    static create(offset: Long, length: Long) {
        return new ArrowBuffer(offset, length);
    }
    constructor(public offset: Long, public length: Long) {
        super();
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitBuffer(this);
    }
}

export class ArrowFieldNode extends ArrowType {
    static create(length: Long, null_count: Long) {
        return new ArrowFieldNode(length, null_count);
    }
    constructor(public length: Long, public null_count: Long) {
        super();
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitFieldNode(this);
    }
}

export class ArrowField extends ArrowType {
    static create(name: string, type: FieldType, typeType: Type, nullable = false,
                  metadata?: Map<string, string>, dictionary?: ArrowDictionaryEncoding,
                  children: ArrowField[] = []) {
        const field = new ArrowField();
        field.name = name;
        field.type = type;
        field.typeType = typeType;
        field.nullable = nullable;
        field.children = children;
        field.metadata = metadata;
        field.dictionary = dictionary;
        return field;
    }
    public typeType: Type;
    public type: FieldType;
    public name?: string;
    public nullable?: boolean;
    public children?: ArrowField[];
    public metadata?: Map<string, string>;
    public dictionary?: ArrowDictionaryEncoding;
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitField(this);
    }
}

export class ArrowDictionaryEncoding extends ArrowType {
    static create(dictionaryId: Long, isOrdered = false) {
        return new ArrowDictionaryEncoding(dictionaryId, isOrdered);
    }
    public indexType: number;
    constructor(public dictionaryId: Long, public isOrdered = false) {
        super();
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitDictionaryEncoding(this);
    }
}

declare module './visitor' {
    namespace ArrowType {
        export let field: typeof ArrowField.create;
        export let schema: typeof ArrowSchema.create;
        export let buffer: typeof ArrowBuffer.create;
        export let fieldNode: typeof ArrowFieldNode.create;
        export let recordBatch: typeof ArrowRecordBatch.create;
        export let dictionaryBatch: typeof ArrowDictionaryBatch.create;
        export let dictionaryEncoding: typeof ArrowDictionaryEncoding.create;
    }
    interface ArrowTypeVisitor {
        visitField(node: ArrowField): any;
        visitSchema(node: ArrowSchema): any;
        visitBuffer(node: ArrowBuffer): any;
        visitFieldNode(node: ArrowFieldNode): any;
        visitRecordBatch(node: ArrowRecordBatch): any;
        visitDictionaryBatch(node: ArrowDictionaryBatch): any;
        visitDictionaryEncoding(node: ArrowDictionaryEncoding): any;
    }
}

ArrowType.field = ArrowField.create;
ArrowType.schema = ArrowSchema.create;
ArrowType.buffer = ArrowBuffer.create;
ArrowType.fieldNode = ArrowFieldNode.create;
ArrowType.recordBatch = ArrowRecordBatch.create;
ArrowType.dictionaryBatch = ArrowDictionaryBatch.create;
ArrowType.dictionaryEncoding = ArrowDictionaryEncoding.create;