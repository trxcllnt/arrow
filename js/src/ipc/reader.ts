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

// import { Vector } from '../interfaces';
// import { flatbuffers } from 'flatbuffers';
// import { Schema, Long } from '../schema';
// import { RecordBatch } from '../recordbatch';
// import { isAsyncIterable } from '../util/compat';
// import { Message, DictionaryBatch, RecordBatchMetadata } from '../ipc/metadata';

import { Data } from '../data';
import * as type from '../type';
import { DataType } from '../type';
import { Vector } from '../vector';
import { Visitor } from '../visitor';
import { magicAndPadding } from './magic';
import { RecordBatch } from '../recordbatch';
import { IsSync, Asyncify } from '../interfaces';
import { ITERATOR_DONE } from '../io/interfaces';
import { MessageHeader, UnionMode } from '../enum';
import { MessageReader, AsyncMessageReader } from './message';

import { Footer } from './metadata/file';
import { Schema, Field } from '../schema';
import { Message, BufferRegion, FieldNode } from './metadata/message';

import {
    ArrowFile, AsyncArrowFile,
    ArrowInput, AsyncArrowInput,
    ArrowStream, AsyncArrowStream,
    ArrowIPCInput, resolveInputFormat,
    InputResolver, AsyncInputResolver,
} from './input';

type RecordBatchReaders = RecordBatchFileReader   | AsyncRecordBatchFileReader   |
                          RecordBatchStreamReader | AsyncRecordBatchStreamReader ;

const FileReaderImpl = (input: ArrowFile | AsyncArrowFile) => input.isSync() ? new RecordBatchFileReader(input) : new AsyncRecordBatchFileReader(input);
const StreamReaderImpl = (input: ArrowStream | AsyncArrowStream) => input.isSync() ? new RecordBatchStreamReader(input) : new AsyncRecordBatchStreamReader(input);
const RecordBatchReaderImpl = (input: AsyncArrowInput | ArrowInput | null): RecordBatchReaders | Promise<RecordBatchReaders> => {
    if (input && input.isFile()) { return FileReaderImpl(input).open(); }
    if (input && input.isStream()) { return StreamReaderImpl(input).open(); }
    return new RecordBatchStreamReader(new ArrowStream(function*(): any {}()));
};

export class ArrowDataSource implements IsSync<ArrowDataSource> {
    private resolver: InputResolver | AsyncInputResolver;
    constructor(source: ArrowIPCInput) {
        this.resolver = resolveInputFormat(source);
    }
    isSync(): this is ArrowDataSource { return this.resolver.isSync(); }
    isAsync(): this is Asyncify<ArrowDataSource> { return this.resolver.isAsync(); }
    open(this: ArrowDataSource): RecordBatchReaders;
    open(this: Asyncify<ArrowDataSource>): Promise<RecordBatchReaders>;
    open() {
        const resolver = this.resolver;
        return resolver.isSync()
            ? RecordBatchReaderImpl(resolver.resolve())
            : resolver.resolve().then(RecordBatchReaderImpl);
    }
}

abstract class AbstractRecordBatchReader<TSource extends MessageReader | AsyncMessageReader> {

    // @ts-ignore
    protected _schema: Schema;
    // @ts-ignore
    protected _footer: Footer;
    protected _numDictionaries = 0;
    protected _numRecordBatches = 0;
    protected _recordBatchIndex = 0;
    protected _dictionaries: Map<number, Vector>;
    public get schema() { return this._schema; }
    public get dictionaries() { return this._dictionaries; }
    public get numDictionaries() { return this._numDictionaries; }
    public get numRecordBatches() { return this._numRecordBatches; }

    constructor(protected source: TSource, dictionaries = new Map<number, Vector>()) {
        this._dictionaries = dictionaries;
    }

    public [Symbol.iterator]() { return this; }
    public [Symbol.asyncIterator]() { return this; }
    public    throw(value?: any) { return this.source.throw(value) as ReturnType<TSource['throw']>;      }
    public   return(value?: any) { return this.source.return(value) as ReturnType<TSource['return']>;    }
    public   isSync(): this is RecordBatchReader       { return this instanceof RecordBatchReader;       }
    public   isFile(): this is RecordBatchFileReader   { return this instanceof RecordBatchFileReader;   }
    public  isAsync(): this is AsyncRecordBatchReader  { return this instanceof AsyncRecordBatchReader;  }
    public isStream(): this is RecordBatchStreamReader { return this instanceof RecordBatchStreamReader; }

    public abstract open(): this | Promise<this>;
    public abstract read(): Message | null | Promise<Message | null>;
    public abstract readSchema(): Schema | null | Promise<Schema | null>;
    public abstract next(value?: any): IteratorResult<RecordBatch> | Promise<IteratorResult<RecordBatch>>;
}

class RecordBatchReader extends AbstractRecordBatchReader<MessageReader>
                        implements Required<IterableIterator<RecordBatch | null | void>> {
    public open() { this.readSchema(); return this; }
    public readSchema() {
        return this._schema || (this._schema = this.source.readSchema());
    }
    public read() {
        const message = this.source.next().value;
        if (message && message.headerType === MessageHeader.DictionaryBatch) {
            this._numDictionaries++;
        } else if (message && message.headerType === MessageHeader.RecordBatch) {
            this._numRecordBatches++;
            this._recordBatchIndex++;
        }
        return message;
    }
    public next() {
        let message: Message, schema = this._schema;
        while (message = this.read()) {
            switch (message.headerType) {
                case MessageHeader.Schema:
                    this._numDictionaries = 0;
                    this._numRecordBatches = 0;
                    this._recordBatchIndex = 0;
                    this._dictionaries = new Map();
                    this._schema = message.header()! as Schema;
                    return { done: true, value: this._schema };
                case MessageHeader.RecordBatch:
                    return { done: false, value: readRecordBatch(this, schema, message) };
                case MessageHeader.DictionaryBatch: readDictionaryBatch(this, schema, message);
            }
        }
        return ITERATOR_DONE;
    }
}

class AsyncRecordBatchReader extends AbstractRecordBatchReader<AsyncMessageReader>
                             implements Required<AsyncIterableIterator<RecordBatch | null | void>> {
    public async open() { await this.readSchema(); return this; }
    public async readSchema() {
        return this._schema || (this._schema = await this.source.readSchema());
    }
    public async read() {
        const message = (await this.source.next()).value;
        if (message && message.headerType === MessageHeader.DictionaryBatch) {
            this._numDictionaries++;
        } else if (message && message.headerType === MessageHeader.RecordBatch) {
            this._recordBatchIndex++;
            this._numRecordBatches++;
        }
        return message;
    }
    public async next() {
        let message: Message, schema = this._schema;
        while (message = await this.read()) {
            switch (message.headerType) {
                case MessageHeader.Schema:
                    this._numDictionaries = 0;
                    this._numRecordBatches = 0;
                    this._recordBatchIndex = 0;
                    this._dictionaries = new Map();
                    this._schema = message.header()! as Schema;
                    return { done: true, value: this._schema };
                case MessageHeader.RecordBatch:
                    return { done: false, value: readRecordBatch(this, schema, message) };
                case MessageHeader.DictionaryBatch: readDictionaryBatch(this, schema, message);
            }
        }
        return ITERATOR_DONE;
    }
}

export class RecordBatchStreamReader extends RecordBatchReader {
    constructor(input: ArrowStream) { super(new MessageReader(input)); }
}

export class AsyncRecordBatchStreamReader extends AsyncRecordBatchReader {
    constructor(input: AsyncArrowStream) { super(new AsyncMessageReader(input)); }
}

export class RecordBatchFileReader extends RecordBatchReader {
    public get footer() { return this._footer; }
    public get numDictionaries() { return this._footer.numDictionaries; }
    public get numRecordBatches() { return this._footer.numRecordBatches; }
    constructor(protected file: ArrowFile) { super(new MessageReader(file)); }
    public readSchema() {
        return this._schema || (this._schema = this.readFooter().schema);
    }
    public read() {
        const block = this._footer.getRecordBatch(this._recordBatchIndex);
        if (block && this.file.seek(block.offset)) {
            return super.read();
        }
        return null; 
    }
    public readRecordBatch(index: number) {
        const block = this._footer.getRecordBatch(index);
        if (block && this.file.seek(block.offset)) {
            const message = this.source.readMessage(MessageHeader.RecordBatch);
            if (message) { return readRecordBatch(this, this._schema, message); }
        }
        return null;
    }
    public readFooter() {
        if (!this._footer) {
            const { file } = this;
            const size = file.size;
            const offset = size - magicAndPadding;
            const length = file.readInt32(offset);
            const footer = this._footer = Footer.decode(file.readAt(offset - length, length));
            for (const { offset } of footer.dictionaryBatches()) {
                file.seek(offset) && this.read();
            }
        }
        return this._footer;
    }
}

export class AsyncRecordBatchFileReader extends AsyncRecordBatchReader {
    public get footer() { return this._footer; }
    public get numDictionaries() { return this._footer.numDictionaries; }
    public get numRecordBatches() { return this._footer.numRecordBatches; }
    constructor(protected file: AsyncArrowFile) { super(new AsyncMessageReader(file)); }
    public async readSchema() {
        return this._schema || (this._schema = (await this.readFooter()).schema);
    }
    public async read() {
        const block = this._footer.getRecordBatch(this._recordBatchIndex);
        if (block && (await this.file.seek(block.offset))) {
            return await super.read();
        }
        return null; 
    }
    public async readRecordBatch(index: number) {
        const block = this._footer.getRecordBatch(index);
        if (block && (await this.file.seek(block.offset))) {
            const message = await this.source.readMessage(MessageHeader.RecordBatch);
            if (message) { return readRecordBatch(this, this._schema, message); }
        }
        return null;
    }
    public async readFooter() {
        if (!this._footer) {
            const { file } = this;
            const offset = file.size - magicAndPadding;
            const length = await file.readInt32(offset);
            const footer = this._footer = Footer.decode(await file.readAt(offset - length, length));
            for (const { offset } of footer.dictionaryBatches()) {
                (await file.seek(offset)) && (await this.read());
            }
        }
        return this._footer;
    }
}

function readRecordBatch(reader: RecordBatchReader | AsyncRecordBatchReader, schema: Schema, message: Message<MessageHeader.RecordBatch>) {
    const { body } = message, { dictionaries } = reader;
    const { nodes, buffers, length } = message.header()!;
    const loader = new DataLoader(body, nodes, buffers, dictionaries);
    return RecordBatch.new(schema, length, loader.visitMany(schema.fields));
}

function readDictionaryBatch(reader: RecordBatchReader | AsyncRecordBatchReader, schema: Schema, message: Message<MessageHeader.DictionaryBatch>) {
    const { body } = message, { dictionaries } = reader;
    const { id, nodes, buffers, isDelta } = message.header()!;
    const loader = new DataLoader(body, nodes, buffers, dictionaries);
    const vector = Vector.new(loader.visit(schema.dictionaries.get(id)!));
    if (isDelta && dictionaries.has(id)) {
        // todo (ptaylor): turn this back on once Vector.concat is implemented
        // vector = _dictionaries.get(id)!.concat(vector);
    }
    dictionaries.set(id, vector);
}

interface DataLoader extends Visitor {
    visitMany <T extends DataType>(fields: Field[]): Data<T>[];
    visit     <T extends DataType>(node: T,       ): Data<T>;
}

class DataLoader extends Visitor {
    private bytes: Uint8Array;
    private nodes: Iterator<FieldNode>;
    private buffers: Iterator<BufferRegion>;
    private dictionaries: Map<number, Vector>;
    constructor(bytes: Uint8Array,
                nodes: FieldNode[],
                buffers: BufferRegion[],
                dictionaries: Map<number, Vector>) {
        super();
        this.bytes = bytes;
        this.dictionaries = dictionaries;
        this.nodes = nodes[Symbol.iterator]();
        this.buffers = buffers[Symbol.iterator]();
    }

    public visitMany(fields: Field[]) { return fields.map((field) => this.visit(field.type)); }

    public visitNull                 <T extends type.Null>                (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Null(type, 0, length, nullCount, this.readNullBitmap(type, nullCount));                                                                                }
    public visitBool                 <T extends type.Bool>                (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Bool(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitInt                  <T extends type.Int>                 (type: T, { length, nullCount } = this.nextFieldNode()) { return             Data.Int(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitFloat                <T extends type.Float>               (type: T, { length, nullCount } = this.nextFieldNode()) { return           Data.Float(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitUtf8                 <T extends type.Utf8>                (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Utf8(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readData(type));                                   }
    public visitBinary               <T extends type.Binary>              (type: T, { length, nullCount } = this.nextFieldNode()) { return          Data.Binary(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readData(type));                                   }
    public visitFixedSizeBinary      <T extends type.FixedSizeBinary>     (type: T, { length, nullCount } = this.nextFieldNode()) { return Data.FixedSizeBinary(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitDate                 <T extends type.Date_>               (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Date(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitTimestamp            <T extends type.Timestamp>           (type: T, { length, nullCount } = this.nextFieldNode()) { return       Data.Timestamp(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitTime                 <T extends type.Time>                (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.Time(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitDecimal              <T extends type.Decimal>             (type: T, { length, nullCount } = this.nextFieldNode()) { return         Data.Decimal(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitList                 <T extends type.List>                (type: T, { length, nullCount } = this.nextFieldNode()) { return            Data.List(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.visitMany(type.children));                         }
    public visitStruct               <T extends type.Struct>              (type: T, { length, nullCount } = this.nextFieldNode()) { return          Data.Struct(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.visitMany(type.children));                                                 }
    public visitUnion                <T extends type.Union>               (type: T                                              ) { return type.mode === UnionMode.Sparse ? this.visitSparseUnion(type as type.SparseUnion) : this.visitDenseUnion(type as type.DenseUnion);                                      }
    public visitDenseUnion           <T extends type.DenseUnion>          (type: T, { length, nullCount } = this.nextFieldNode()) { return           Data.Union(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readTypeIds(type), this.visitMany(type.children)); }
    public visitSparseUnion          <T extends type.SparseUnion>         (type: T, { length, nullCount } = this.nextFieldNode()) { return           Data.Union(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), null!, this.readTypeIds(type), this.visitMany(type.children));                  }
    public visitDictionary           <T extends type.Dictionary>          (type: T, { length, nullCount } = this.nextFieldNode()) { return      Data.Dictionary(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)), this.dictionaries.get(type.id!);                          }
    public visitInterval             <T extends type.Interval>            (type: T, { length, nullCount } = this.nextFieldNode()) { return        Data.Interval(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type));                                                           }
    public visitFixedSizeList        <T extends type.FixedSizeList>       (type: T, { length, nullCount } = this.nextFieldNode()) { return   Data.FixedSizeList(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.visitMany(type.children));                                                 }
    public visitMap                  <T extends type.Map_>                (type: T, { length, nullCount } = this.nextFieldNode()) { return             Data.Map(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.visitMany(type.children));                                                 }

    protected nextFieldNode() { return this.nodes.next().value; }
    protected nextBufferRange() { return this.buffers.next().value; }
    protected readNullBitmap<T extends DataType>(type: T, nullCount: number, buffer = this.nextBufferRange()) {
        return nullCount > 0 && this.readData(type, buffer) || new Uint8Array(0);
    }
    protected readOffsets<T extends DataType>(type: T, buffer?: BufferRegion) { return this.readData(type, buffer); }
    protected readTypeIds<T extends DataType>(type: T, buffer?: BufferRegion) { return this.readData(type, buffer); }
    protected readData<T extends DataType>(_type: T, { length, offset } = this.nextBufferRange()) {
        return new Uint8Array(this.bytes.buffer, this.bytes.byteOffset + offset, length);
    }
}
