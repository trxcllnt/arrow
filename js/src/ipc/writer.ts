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

import { Table } from '../table';
import { MAGIC } from './message';
import { Vector } from '../vector';
import { Column } from '../column';
import { Schema, Field } from '../schema';
import { Chunked } from '../vector/chunked';
import { Message } from './metadata/message';
import { RecordBatch } from '../recordbatch';
import * as metadata from './metadata/message';
import { DataType, Dictionary } from '../type';
import { FileBlock, Footer } from './metadata/file';
import { MessageHeader, MetadataVersion } from '../enum';
import { WritableSink, AsyncByteQueue } from '../io/stream';
import { VectorAssembler } from '../visitor/vectorassembler';
import { JSONTypeAssembler } from '../visitor/jsontypeassembler';
import { JSONVectorAssembler } from '../visitor/jsonvectorassembler';
import { ArrayBufferViewInput, toUint8Array } from '../util/buffer';
import { isWritableDOMStream, isWritableNodeStream, isAsyncIterable } from '../util/compat';
import { Writable, FileHandle, ReadableInterop, ReadableDOMStreamOptions } from '../io/interfaces';

const kAlignmentBytes = new Uint8Array(64).fill(0);

export type OpenArgs = FileHandle | NodeJS.WritableStream | WritableStream<Uint8Array> | UnderlyingSink<Uint8Array>;

export class RecordBatchWriter<T extends { [key: string]: DataType } = any> extends ReadableInterop<Uint8Array> implements Writable<RecordBatch<T>> {

    /** @nocollapse */
    public static throughNode(): import('stream').Duplex { throw new Error(`"throughNode" not available in this environment`); }
    /** @nocollapse */
    public static throughDOM<T extends { [key: string]: DataType }>(): { writable: WritableStream<RecordBatch<T>>, readable: ReadableStream<Uint8Array> } {
        throw new Error(`"throughDOM" not available in this environment`);
    }

    protected _position = 0;
    protected _started = false;
    // @ts-ignore
    protected _sink = new AsyncByteQueue();
    protected _schema: Schema | null = null;
    protected _dictionaryBlocks: FileBlock[] = [];
    protected _recordBatchBlocks: FileBlock[] = [];

    public toUint8Array(sync: true): Uint8Array;
    public toUint8Array(sync?: false): Promise<Uint8Array>;
    public toUint8Array(sync: any = false) {
        return this._sink.toUint8Array(sync) as Promise<Uint8Array> | Uint8Array;
    }

    public get closed() { return this._sink.closed; }
    public [Symbol.asyncIterator]() { return this._sink[Symbol.asyncIterator](); }
    public toReadableDOMStream(options?: ReadableDOMStreamOptions) { return this._sink.toReadableDOMStream(options); }
    public toReadableNodeStream(options?: import('stream').ReadableOptions) { return this._sink.toReadableNodeStream(options); }

    public close() { return this.reset()._sink.close(); }
    public abort(reason?: any) { return this.reset()._sink.abort(reason); }
    public reset(sink: WritableSink<ArrayBufferViewInput> = this._sink, schema?: Schema<T>) {

        if ((sink === this._sink) || (sink instanceof AsyncByteQueue)) {
            this._sink = sink as AsyncByteQueue;
        } else {
            this._sink = new AsyncByteQueue();
            if (sink && isWritableDOMStream(sink)) {
                this.toReadableDOMStream().pipeTo(sink);
            } else if (sink && isWritableNodeStream(sink)) {
                this.toReadableNodeStream().pipe(sink);
            }
        }

        this._position = 0;
        this._schema = null;
        this._started = false;
        this._dictionaryBlocks = [];
        this._recordBatchBlocks = [];

        if (schema instanceof Schema) {
            this._started = true;
            this._schema = schema;
            this._writeSchema(schema);
        }

        return this;
    }

    public write(chunk: RecordBatch<T>) {
        if (!this._sink) {
            throw new Error(`RecordBatchWriter is closed`);
        }
        if (!this._started && (this._started = true)) {
            this._writeSchema(this._schema = chunk.schema);
        }
        if (chunk.schema !== this._schema) {
            throw new Error('Schemas unequal');
        }
        this._writeRecordBatch(chunk);
    }

    protected _writeMessage<T extends MessageHeader>(message: Message<T>, alignment = 8) {

        const a = alignment - 1;
        const buffer = Message.encode(message);
        const flatbufferSize = buffer.byteLength;
        const alignedSize = (flatbufferSize + 4 + a) & ~a;
        const nPaddingBytes = alignedSize - flatbufferSize - 4;

        if (message.headerType === MessageHeader.RecordBatch) {
            this._recordBatchBlocks.push(new FileBlock(alignedSize, message.bodyLength, this._position));
        } else if (message.headerType === MessageHeader.DictionaryBatch) {
            this._dictionaryBlocks.push(new FileBlock(alignedSize, message.bodyLength, this._position));
        }

        // Write the flatbuffer size prefix including padding
        this._write(Int32Array.of(alignedSize - 4));
        // Write the flatbuffer
        if (flatbufferSize > 0) { this._write(buffer); }
        // Write any padding
        return this._writePadding(nPaddingBytes);
    }

    protected _write(chunk: ArrayBufferViewInput) {
        const buffer = toUint8Array(chunk);
        if (buffer && buffer.byteLength > 0) {
            this._sink.write(buffer);
            this._position += buffer.byteLength;
        }
        return this;
    }

    protected _writeSchema(schema: Schema<T>) {
        return this
            ._writeMessage(Message.from(schema))
            ._writeDictionaries(schema.dictionaryFields);
    }

    protected _writeFooter() {

        const buffer = Footer.encode(new Footer(
            this._schema!, MetadataVersion.V4,
            this._recordBatchBlocks, this._dictionaryBlocks
        ));

        return this
            ._write(buffer) // Write the flatbuffer
            ._write(Int32Array.of(buffer.byteLength)) // then the footer size suffix
            ._writeMagic(); // then the magic suffix
    }

    protected _writeMagic() {
        return this._write(MAGIC);
    }

    protected _writePadding(nBytes: number) {
        return nBytes > 0 ? this._write(kAlignmentBytes.subarray(0, nBytes)) : this;
    }

    protected _writeRecordBatch(records: RecordBatch<T>) {
        const { byteLength, nodes, bufferRegions, buffers } = VectorAssembler.assemble(records);
        const recordBatch = new metadata.RecordBatch(records.length, nodes, bufferRegions);
        const message = Message.from(recordBatch, byteLength);
        return this
            ._writeMessage(message)
            ._writeBodyBuffers(buffers);
    }

    protected _writeDictionaryBatch(dictionary: Vector, id: number, isDelta = false) {
        const { byteLength, nodes, bufferRegions, buffers } = VectorAssembler.assemble(dictionary);
        const recordBatch = new metadata.RecordBatch(dictionary.length, nodes, bufferRegions);
        const dictionaryBatch = new metadata.DictionaryBatch(recordBatch, id, isDelta);
        const message = Message.from(dictionaryBatch, byteLength);
        return this
            ._writeMessage(message)
            ._writeBodyBuffers(buffers);
    }

    protected _writeBodyBuffers(buffers: ArrayBufferView[]) {
        let buffer: ArrayBufferView;
        let size: number, padding: number;
        for (let i = -1, n = buffers.length; ++i < n;) {
            if ((buffer = buffers[i]) && (size = buffer.byteLength) > 0) {
                this._write(buffer);
                if ((padding = ((size + 7) & ~7) - size) > 0) {
                    this._writePadding(padding);
                }
            }
        }
        return this;
    }

    protected _writeDictionaries(dictionaryFields: Map<number, Field<Dictionary<any, any>>[]>) {
        for (const [id, fields] of dictionaryFields) {
            const vector = fields[0].type.dictionaryVector;
            if (!(vector instanceof Chunked)) {
                this._writeDictionaryBatch(vector, id, false);
            } else {
                const chunks = vector.chunks;
                for (let i = -1, n = chunks.length; ++i < n;) {
                    this._writeDictionaryBatch(chunks[i], id, i > 0);
                }
            }
        }
        return this;
    }
}

export class RecordBatchFileWriter<T extends { [key: string]: DataType } = any> extends RecordBatchWriter<T> {

    public static writeAll<T extends { [key: string]: DataType } = any>(input: Table<T> | Iterable<RecordBatch<T>>): RecordBatchFileWriter<T>;
    public static writeAll<T extends { [key: string]: DataType } = any>(input: AsyncIterable<RecordBatch<T>>): Promise<RecordBatchFileWriter<T>>;
    /** @nocollapse */
    public static writeAll<T extends { [key: string]: DataType } = any>(input: Table<T> | Iterable<RecordBatch<T>> | AsyncIterable<RecordBatch<T>>) {
        return !isAsyncIterable(input) ? writeAll(new RecordBatchFileWriter<T>(), input) : writeAllAsync(new RecordBatchFileWriter<T>(), input);
    }
    public close() {
        this._writeFooter();
        return super.close();
    }
    protected _writeSchema(schema: Schema<T>) {
        return this
            ._writeMagic()._writePadding(2)
            ._writeDictionaries(schema.dictionaryFields);
    }
}

export class RecordBatchStreamWriter<T extends { [key: string]: DataType } = any> extends RecordBatchWriter<T> {

    public static writeAll<T extends { [key: string]: DataType } = any>(input: Table<T> | Iterable<RecordBatch<T>>): RecordBatchStreamWriter<T>;
    public static writeAll<T extends { [key: string]: DataType } = any>(input: AsyncIterable<RecordBatch<T>>): Promise<RecordBatchStreamWriter<T>>;
    /** @nocollapse */
    public static writeAll<T extends { [key: string]: DataType } = any>(input: Table<T> | Iterable<RecordBatch<T>> | AsyncIterable<RecordBatch<T>>) {
        return !isAsyncIterable(input) ? writeAll(new RecordBatchStreamWriter<T>(), input) : writeAllAsync(new RecordBatchStreamWriter<T>(), input);
    }
    public close() {
        this._writePadding(4);
        return super.close();
    }
}

export class RecordBatchJSONWriter<T extends { [key: string]: DataType } = any> extends RecordBatchWriter<T> {
    public static writeAll<T extends { [key: string]: DataType } = any>(input: Table<T> | Iterable<RecordBatch<T>>): RecordBatchJSONWriter<T>;
    public static writeAll<T extends { [key: string]: DataType } = any>(input: AsyncIterable<RecordBatch<T>>): Promise<RecordBatchJSONWriter<T>>;
    /** @nocollapse */
    public static writeAll<T extends { [key: string]: DataType } = any>(input: Table<T> | Iterable<RecordBatch<T>> | AsyncIterable<RecordBatch<T>>) {
        return !isAsyncIterable(input) ? writeAll(new RecordBatchJSONWriter<T>(), input) : writeAllAsync(new RecordBatchJSONWriter<T>(), input);
    }
    protected _writeMessage() { return this; }
    protected _writeSchema(schema: Schema<T>) {
        return this._write(`{\n  "schema": ${
            JSON.stringify({ fields: schema.fields.map(fieldToJSON) }, null, 2)
        }`)._writeDictionaries(schema.dictionaryFields);
    }
    protected _writeDictionaries(dictionaryFields: Map<number, Field<Dictionary<any, any>>[]>) {
        this._write(`,\n  "dictionaries": [\n`);
        super._writeDictionaries(dictionaryFields);
        return this._write(`\n  ]`);
    }
    protected _writeDictionaryBatch(dictionary: Vector, id: number, isDelta = false) {
        this._write(this._dictionaryBlocks.length === 0 ? `    ` : `,\n    `);
        this._write(`${dictionaryBatchToJSON(this._schema!, dictionary, id, isDelta)}`);
        this._dictionaryBlocks.push(new FileBlock(0, 0, 0));
        return this;
    }
    protected _writeRecordBatch(records: RecordBatch<T>) {
        this._write(this._recordBatchBlocks.length === 0
            ? `,\n  "batches": [\n    `
            : `,\n    `);
        this._write(`${recordBatchToJSON(records)}`);
        this._recordBatchBlocks.push(new FileBlock(0, 0, 0));
        return this;
    }
    public close() {
        if (this._recordBatchBlocks.length > 0) {
            this._write(`\n  ]`);
        }
        if (this._schema) {
            this._write(`\n}`);
        }
        return super.close();
    }
}

function writeAll<T extends RecordBatchWriter<R>, R extends { [key: string]: DataType } = any>(writer: T, input: Table<R> | Iterable<RecordBatch<R>>) {
    const chunks = (input instanceof Table) ? input.chunks : input;
    for (const batch of chunks) { writer.write(batch); }
    writer.close();
    return writer;
}

async function writeAllAsync<T extends RecordBatchWriter<R>, R extends { [key: string]: DataType } = any>(writer: T, batches: AsyncIterable<RecordBatch<R>>) {
    for await (const batch of batches) { writer.write(batch); }
    writer.close();
    return writer;
}

function fieldToJSON({ name, type, nullable }: Field): object {
    const assembler = new JSONTypeAssembler();
    return {
        'name': name, 'nullable': nullable,
        'type': assembler.visit(type),
        'children': (type.children || []).map(fieldToJSON),
        'dictionary': !DataType.isDictionary(type) ? undefined : {
            'id': type.id,
            'isOrdered': type.isOrdered,
            'indexType': assembler.visit(type.indices)
        }
    };
}

function dictionaryBatchToJSON(schema: Schema, dictionary: Vector, id: number, isDelta = false) {
    const f = schema.dictionaryFields.get(id)![0];
    const field = new Field(f.name, f.type.dictionary, f.nullable, f.metadata);
    const columns = JSONVectorAssembler.assemble(new Column(field, [dictionary]));
    return JSON.stringify({
        'id': id,
        'isDelta': isDelta,
        'data': {
            'count': dictionary.length,
            'columns': columns
        }
    }, null, 2);
}

function recordBatchToJSON(records: RecordBatch) {
    return JSON.stringify({
        'count': records.length,
        'columns': JSONVectorAssembler.assemble(records)
    }, null, 2);
}
