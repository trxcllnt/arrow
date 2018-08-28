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

import { Data } from '../../data';
import { Vector } from '../../vector';
import { TypeVisitor } from '../../visitor';
import { RecordBatch } from '../../recordbatch';
import { Message, FieldMetadata, BufferMetadata } from '../metadata';
import {
    Schema, Field, DataType,
    Dictionary,
    Null, Int, Float,
    Binary, Bool, Utf8, Decimal,
    Date_, Time, Timestamp, Interval,
    List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
    UnionMode,
} from '../../type';

import {
    NullVector,
    IntVector,
    FloatVector,
    BinaryVector,
    Utf8Vector,
    BoolVector,
    DecimalVector,
    DateVector,
    TimeVector,
    TimestampVector,
    IntervalVector,
    ListVector,
    StructVector,
    UnionVector,
    FixedSizeBinaryVector,
    FixedSizeListVector,
    MapVector,
    DictionaryVector
} from '../../vector';

export function* readRecordBatches(messages: Iterable<{ schema: Schema, message: Message, loader: TypeDataLoader }>) {
    for (const { schema, message, loader } of messages) {
        yield* readRecordBatch(schema, message, loader);
    }
}

export async function* readRecordBatchesAsync(messages: AsyncIterable<{ schema: Schema, message: Message, loader: TypeDataLoader }>) {
    for await (const { schema, message, loader } of messages) {
        yield* readRecordBatch(schema, message, loader);
    }
}

export function* readRecordBatch(schema: Schema, message: Message, loader: TypeDataLoader) {
    if (Message.isRecordBatch(message)) {
        yield new RecordBatch(schema, message.length, loader.visitFields(schema.fields));
    } else if (Message.isDictionaryBatch(message)) {
        const dictionaryId = message.id;
        const dictionaries = loader.dictionaries;
        const dictionaryField = schema.dictionaries.get(dictionaryId)!;
        const dictionaryDataType = (dictionaryField.type as Dictionary).dictionary;
        let dictionaryVector = Vector.create(loader.visit(dictionaryDataType));
        if (message.isDelta && dictionaries.has(dictionaryId)) {
            dictionaryVector = dictionaries.get(dictionaryId)!.concat(dictionaryVector);
        }
        dictionaries.set(dictionaryId, dictionaryVector);
    }
}

export abstract class TypeDataLoader extends TypeVisitor {

    public dictionaries: Map<number, Vector>;
    protected nodes: Iterator<FieldMetadata>;
    protected buffers: Iterator<BufferMetadata>;

    constructor(nodes: Iterator<FieldMetadata>, buffers: Iterator<BufferMetadata>, dictionaries: Map<number, Vector>) {
        super();
        this.nodes = nodes;
        this.buffers = buffers;
        this.dictionaries = dictionaries;
    }

    public visitFields(fields: Field[]) { return fields.map((field) => this.visit(field.type)); }
                                                                                                                        // this.dictionaries.get(type.id)!
    public visitNull           (type: Null,            { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return             new NullVector(           Data.Null(type, 0, length, nullCount, this.readNullBitmap(type, nullCount)));                                                          }
    public visitInt            (type: Int,             { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return              new IntVector(            Data.Int(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitFloat          (type: Float,           { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return            new FloatVector(          Data.Float(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitBool           (type: Bool,            { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return             new BoolVector(           Data.Bool(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitDecimal        (type: Decimal,         { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return          new DecimalVector(        Data.Decimal(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitDate           (type: Date_,           { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return             new DateVector(           Data.Date(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitTime           (type: Time,            { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return             new TimeVector(           Data.Time(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitTimestamp      (type: Timestamp,       { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return        new TimestampVector(      Data.Timestamp(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitInterval       (type: Interval,        { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return         new IntervalVector(       Data.Interval(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitFixedSizeBinary(type: FixedSizeBinary, { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return  new FixedSizeBinaryVector(Data.FixedSizeBinary(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitDictionary     (type: Dictionary,      { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return       new DictionaryVector(     Data.Dictionary(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readData(type)));                                     }
    public visitBinary         (type: Binary,          { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return           new BinaryVector(         Data.Binary(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readData(type)));             }
    public visitUtf8           (type: Utf8,            { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return             new Utf8Vector(           Data.Utf8(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readData(type)));             }
    public visitList           (type: List,            { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return             new ListVector(           Data.List(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.visitFields(type.children))); }
    public visitFixedSizeList  (type: FixedSizeList,   { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return    new FixedSizeListVector(  Data.FixedSizeList(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.visitFields(type.children)));                         }
    public visitStruct         (type: Struct,          { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return           new StructVector(         Data.Struct(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.visitFields(type.children)));                         }
    public visitMap            (type: Map_,            { length, nullCount }: FieldMetadata = this.getFieldMetadata()) { return              new MapVector(            Data.Map(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.visitFields(type.children)));                         }
    public visitUnion          (type: Union,           { length, nullCount }: FieldMetadata = this.getFieldMetadata()) {
        return new UnionVector(type.mode === UnionMode.Sparse ?
            Data.Union(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), null!, this.readTypeIds(type), this.visitFields(type.children)) :
            Data.Union(type, 0, length, nullCount, this.readNullBitmap(type, nullCount), this.readOffsets(type), this.readTypeIds(type), this.visitFields(type.children)));
    }
    protected getFieldMetadata() { return this.nodes.next().value; }
    protected getBufferMetadata() { return this.buffers.next().value; }
    protected readNullBitmap<T extends DataType>(type: T, nullCount: number, buffer = this.getBufferMetadata()) {
        return nullCount > 0 && this.readData(type, buffer) || new Uint8Array(0);
    }
    protected abstract readData<T extends DataType>(type: T, buffer?: BufferMetadata): any;
    protected abstract readOffsets<T extends DataType>(type: T, buffer?: BufferMetadata): any;
    protected abstract readTypeIds<T extends DataType>(type: T, buffer?: BufferMetadata): any;
}
