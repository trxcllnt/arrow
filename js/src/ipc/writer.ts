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

import { MAGIC } from './message';
import { Vector } from '../vector';
import { Schema, Field } from '../schema';
import { Message } from './metadata/message';
import { RecordBatch } from '../recordbatch';
import * as metadata from './metadata/message';
import { DataType, Dictionary } from '../type';
import { ChunkedVector } from '../vector/chunked';
import { FileBlock, Footer } from './metadata/file';
import { ArrayBufferViewInput } from '../util/buffer';
import { MessageHeader, MetadataVersion } from '../enum';
import { WritableSink, AsyncByteQueue } from '../io/stream';
import { VectorAssembler } from '../visitor/vectorassembler';
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

    protected position = 0;
    protected started = false;
    // @ts-ignore
    protected sink = new AsyncByteQueue();
    protected schema: Schema | null = null;
    protected dictionaryBlocks: FileBlock[] = [];
    protected recordBatchBlocks: FileBlock[] = [];

    public toUint8Array(sync: true): Uint8Array;
    public toUint8Array(sync?: false): Promise<Uint8Array>;
    public toUint8Array(sync: any = false) {
        return this.sink.toUint8Array(sync) as Promise<Uint8Array> | Uint8Array;
    }

    public get closed() { return this.sink.closed; }
    public [Symbol.asyncIterator]() { return this.sink[Symbol.asyncIterator](); }
    public toReadableDOMStream(options?: ReadableDOMStreamOptions) { return this.sink.toReadableDOMStream(options); }
    public toReadableNodeStream(options?: import('stream').ReadableOptions) { return this.sink.toReadableNodeStream(options); }

    public close() { return this.reset().sink.close(); }
    public abort(reason?: any) { return this.reset().sink.abort(reason); }
    public reset(sink: WritableSink<ArrayBufferViewInput> = this.sink, schema?: Schema<T>) {

        if ((sink === this.sink) || (sink instanceof AsyncByteQueue)) {
            this.sink = sink as AsyncByteQueue;
        } else {
            this.sink = new AsyncByteQueue();
            if (sink && isWritableDOMStream(sink)) {
                this.toReadableDOMStream().pipeTo(sink);
            } else if (sink && isWritableNodeStream(sink)) {
                this.toReadableNodeStream().pipe(sink);
            }
        }

        this.position = 0;
        this.schema = null;
        this.started = false;
        this.dictionaryBlocks = [];
        this.recordBatchBlocks = [];

        if (schema instanceof Schema) {
            this.started = true;
            this.schema = schema;
            this._writeSchema(schema);
        }

        return this;
    }

    public write(chunk: RecordBatch<T>) {
        if (!this.sink) {
            throw new Error(`RecordBatchWriter is closed`);
        }
        if (!this.started && (this.started = true)) {
            this._writeSchema(this.schema = chunk.schema);
        }
        if (chunk.schema !== this.schema) {
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
            this.recordBatchBlocks.push(new FileBlock(alignedSize, message.bodyLength, this.position));
        } else if (message.headerType === MessageHeader.DictionaryBatch) {
            this.dictionaryBlocks.push(new FileBlock(alignedSize, message.bodyLength, this.position));
        }

        // Write the flatbuffer size prefix including padding
        this._write(Int32Array.of(alignedSize - 4));
        // Write the flatbuffer
        if (flatbufferSize > 0) { this._write(buffer); }
        // Write any padding
        return this._writePadding(nPaddingBytes);
    }

    protected _write(buffer: ArrayBufferView) {
        if (buffer && buffer.byteLength > 0) {
            this.sink.write(buffer);
            this.position += buffer.byteLength;
        }
        return this;
    }

    protected _writeSchema(schema: Schema<T>) {
        return this
            ._writeMessage(Message.from(schema))
            ._writeDictionaries(schema.dictionaryFields);
    }

    protected _writeFooter() {

        const { schema, recordBatchBlocks, dictionaryBlocks } = this;
        const buffer = Footer.encode(new Footer(
            schema!, MetadataVersion.V4,
            recordBatchBlocks, dictionaryBlocks
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
            if (!(vector instanceof ChunkedVector)) {
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

    public static writeAll<T extends { [key: string]: DataType } = any>(batches: Iterable<RecordBatch<T>>): RecordBatchFileWriter<T>;
    public static writeAll<T extends { [key: string]: DataType } = any>(batches: AsyncIterable<RecordBatch<T>>): Promise<RecordBatchFileWriter<T>>;
    /** @nocollapse */
    public static writeAll<T extends { [key: string]: DataType } = any>(batches: Iterable<RecordBatch<T>> | AsyncIterable<RecordBatch<T>>) {
        const writer = new RecordBatchFileWriter<T>();
        if (!isAsyncIterable(batches)) {
            for (const batch of batches) {
                writer.write(batch);
            }
            writer.close();
            return writer;
        }
        return (async () => {
            for await (const batch of batches) {
                writer.write(batch);
            }
            writer.close();
            return writer;
        })();
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

    public static writeAll<T extends { [key: string]: DataType } = any>(batches: Iterable<RecordBatch<T>>): RecordBatchStreamWriter<T>;
    public static writeAll<T extends { [key: string]: DataType } = any>(batches: AsyncIterable<RecordBatch<T>>): Promise<RecordBatchStreamWriter<T>>;
    /** @nocollapse */
    public static writeAll<T extends { [key: string]: DataType } = any>(batches: Iterable<RecordBatch<T>> | AsyncIterable<RecordBatch<T>>) {
        const writer = new RecordBatchStreamWriter<T>();
        if (!isAsyncIterable(batches)) {
            for (const batch of batches) {
                writer.write(batch);
            }
            writer.close();
            return writer;
        }
        return (async () => {
            for await (const batch of batches) {
                writer.write(batch);
            }
            writer.close();
            return writer;
        })();
    }
    public close() {
        this._writePadding(4);
        return super.close();
    }
}
