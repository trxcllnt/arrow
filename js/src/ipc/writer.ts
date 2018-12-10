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

import { Schema } from '../schema';
import { Vector } from '../vector';
import { MessageHeader } from '../enum';
import { ChunkedVector } from '../column';
import { Message } from './metadata/message';
import { RecordBatch } from '../recordbatch';
import * as metadata from './metadata/message';
import { DataType, Dictionary } from '../type';
import { AsyncWritableByteStream } from '../io/stream';
import { VectorAssembler } from '../visitor/vectorassembler';
import { FileHandle, Streamable, ReadableDOMStreamOptions } from '../io/interfaces';
// import { isFileHandle, isPromise, isWritableDOMStream, isWritableNodeStream, isUnderlyingSink } from '../util/compat';

export type OpenArgs = FileHandle | NodeJS.WritableStream | WritableStream<Uint8Array> | UnderlyingSink<Uint8Array>;

export class RecordBatchWriter<T extends { [key: string]: DataType } = any> extends Streamable<Uint8Array> implements UnderlyingSink<RecordBatch<T>> {

    public static throughNode(): import('stream').Duplex { throw new Error(`"throughNode" not available in this environment`); }
    public static throughDOM<T extends { [key: string]: DataType }>(): { writable: WritableStream<RecordBatch<T>>, readable: ReadableStream<Uint8Array> } {
        throw new Error(`"throughDOM" not available in this environment`);
    }

    constructor(sink?: AsyncWritableByteStream<Uint8Array>) {
        super();
        this.sink = sink || new AsyncWritableByteStream<Uint8Array>()
    }

    protected started = false;
    protected schema: Schema | null = null;
    protected sink: AsyncWritableByteStream<Uint8Array>;

    public [Symbol.asyncIterator]() { return this.sink[Symbol.asyncIterator](); }
    public toReadableDOMStream(options?: ReadableDOMStreamOptions) { return this.sink.toReadableDOMStream(options); }
    public toReadableNodeStream(options?: import('stream').ReadableOptions) { return this.sink.toReadableNodeStream(options); }

    public close() { return this.reset().sink.close(); }
    public abort(reason?: any) { return this.reset().sink.abort(reason); }
    public reset(sink: AsyncWritableByteStream<Uint8Array> = this.sink, schema?: Schema<T>) {
        this.started = false;
        this.schema = <any> schema;
        this.sink = sink || new AsyncWritableByteStream();
        return this;
    }

    public write(chunk: RecordBatch<T>, controller?: WritableStreamDefaultController): void | PromiseLike<void>;
    public write(chunk: RecordBatch<T>, encoding?: string, cb?: (error: Error | null | undefined) => void): void;
    public write(chunk: RecordBatch<T>, ...args: any[]) {
        if (!this.sink) {
            throw new Error(`RecordBatchWriter is closed`)
        }
        if (!this.started && (this.started = true)) {
            this._writeSchema(this.schema = chunk.schema);
        }
        if (chunk.schema !== this.schema) {
            throw new Error('Schemas unequal');
        }
        return this._writeRecordBatch(chunk).sink.align(8, ...args);
    }

    protected _writeMessage<T extends MessageHeader>(message: Message<T>, alignment = 8) {
        const to = alignment - 1;
        const buffer = Message.encode(message);
        const paddedLength = (buffer.byteLength + 4 + to) & ~to;
        const remainder = paddedLength - buffer.byteLength - 4;
        // Write the flatbuffer size prefix including padding
        this.sink.write(Int32Array.of(paddedLength - 4));
        // Write the flatbuffer
        if (buffer.byteLength > 0) { this.sink.write(buffer); }
        // Write any padding
        if (remainder > 0) { this.sink.write(new Uint8Array(remainder)); }
        return this;
    }

    protected _writeSchema(schema: Schema) {
        this._writeMessage(Message.from(schema));
        this._writeDictionaries(schema.dictionaries);
        return this;
    }

    protected _writeRecordBatch(records: RecordBatch) {
        const { byteLength, nodes, bufferRegions, buffers } = VectorAssembler.assemble(records);
        const recordBatch = new metadata.RecordBatch(records.length, nodes, bufferRegions);
        this._writeMessage(Message.from(recordBatch, byteLength));
        this._writeBodyBuffers(buffers);
        return this;
    }

    protected _writeDictionaryBatch(dictionary: Vector, id: number, isDelta = false) {
        const { byteLength, nodes, bufferRegions, buffers } = VectorAssembler.assemble(dictionary);
        const recordBatch = new metadata.RecordBatch(dictionary.length, nodes, bufferRegions);
        const dictionaryBatch = new metadata.DictionaryBatch(recordBatch, id, isDelta);
        this._writeMessage(Message.from(dictionaryBatch, byteLength));
        this._writeBodyBuffers(buffers);
        return this;
    }

    protected _writeBodyBuffers(buffers: ArrayBufferView[]) {
        for (let b, i = -1, n = buffers.length; ++i < n;) {
            if ((b = buffers[i]).byteLength > 0) {
                this.sink.write(b);
                this.sink.align(8);
            }
        }
        return this;
    }

    protected _writeDictionaries(dictionaries: Map<number, Dictionary>) {
        for (const [id, dictionary] of dictionaries) {
            let vector = dictionary.dictionaryVector;
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
