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

import { DataType } from '../../type';
import { Vector } from '../../vector';
import { Schema } from '../../schema';
import { MessageHeader } from '../../enum';
import { Message } from '.././metadata/message';
import { RecordBatch } from '../../recordbatch';
import * as metadata from '.././metadata/message';
import { ITERATOR_DONE } from '../../io/interfaces';
import { VectorLoader } from '../../visitor/vectorloader';
import { AsyncMessageReader } from '../message/asyncmessagereader';
import { AbstractRecordBatchReader } from './abstractrecordbatchreader';
import { ReadableDOMStream, ReadableNodeStream } from '../../io/interfaces';

export abstract class AsyncRecordBatchReader<T extends { [key: string]: DataType } = any>
       extends AbstractRecordBatchReader<AsyncMessageReader>
       implements AsyncIterableIterator<RecordBatch<T>> {

    public [Symbol.asyncIterator](): AsyncIterableIterator<RecordBatch> { return this as any; }
    public async open() { await this.readSchema(); return this; }
    public async readSchema() {
        return this._schema || (this._schema = await this.source.readSchema());
    }
    public async readMessage<T extends MessageHeader>(type?: T | null): Promise<Message<T> | null> {
        const message = await this.source.readMessage(type);
        if (message && message.isDictionaryBatch()) {
            this._dictionaryIndex++;
        } else if (message && message.isRecordBatch()) {
            this._recordBatchIndex++;
        }
        return message;
    }
    public async next(): Promise<IteratorResult<RecordBatch<T>>> {
        let message: Message | null;
        let schema = await this.readSchema();
        let dictionaries = this.dictionaries;
        let header: metadata.RecordBatch | metadata.DictionaryBatch;
        while (message = await this.readMessage()) {
            if (message.isSchema()) {
                this._reset(message.header()! as Schema<T>);
                break;
            } else if (message.isRecordBatch() && (header = message.header()!)) {
                return { done: false, value: await this._loadRecordBatch(schema, message.bodyLength, header) };
            } else if (message.isDictionaryBatch() && (header = message.header()!)) {
                dictionaries.set(header.id, await this._loadDictionaryBatch(schema, message.bodyLength, header));
            }
        }
        return ITERATOR_DONE;
    }
    protected async _loadRecordBatch(schema: Schema<T>, bodyLength: number, header: metadata.RecordBatch) {
        return new RecordBatch<T>(schema, header.length, (await this._vectorLoader(bodyLength, header)).visitMany(schema.fields));
    }
    protected async _loadDictionaryBatch(schema: Schema<T>, bodyLength: number, header: metadata.DictionaryBatch): Promise<Vector> {
        const { dictionaries } = this;
        const { id, isDelta } = header;
        if (isDelta || !dictionaries.get(id)) {
            const type = schema.dictionaries.get(id)!;
            const loader = await this._vectorLoader(bodyLength, header);
            const vector = isDelta ? dictionaries.get(id)!.concat(
                Vector.new(loader.visit(type))) as any :
                Vector.new(loader.visit(type)) ;
            return (type.dictionaryVector = vector);
        }
        return dictionaries.get(id)!;
    }
    protected async _vectorLoader(bodyLength: number, metadata: metadata.RecordBatch | metadata.DictionaryBatch) {
        return new VectorLoader(await this.source.readMessageBody(bodyLength), metadata.nodes, metadata.buffers);
    }
    public asReadableDOMStream() {
        let self = this, it: AsyncIterator<RecordBatch>;
        return new ReadableDOMStream<RecordBatch>({
            cancel: close.bind(null, 'return'),
            async start() { it = self[Symbol.asyncIterator](); },
            async pull(controller) {
                try {
                    let size = controller.desiredSize;
                    let r: IteratorResult<RecordBatch> | null = null;
                    while ((size == null || size > 0) && !(r = await it.next()).done) {
                        controller.enqueue(r.value);
                    }
                    r && r.done && (await Promise.all([close('return'), controller.close()]));
                } catch (e) {
                    await Promise.all([close('throw', e), controller.error(e)]);
                }
            }
        });
        async function close(signal: 'throw' | 'return', value?: any) {
            if (it && typeof it[signal] === 'function') {
                await it[signal]!(value);
            }
        }
    }
    public asReadableNodeStream() {
        let self = this, it: AsyncIterator<RecordBatch>;
        return new ReadableNodeStream({
            objectMode: true,
            read(size: number) {
                (it || (it = self[Symbol.asyncIterator]())) && read(this, size);
            },
            destroy(e: Error | null, cb: (e: Error | null) => void) {
                close(e == null ? 'return' : 'throw', e).then(cb as any, cb);
            }
        });
        async function read(sink: ReadableNodeStream, size: number) {
            let r: IteratorResult<RecordBatch> | null = null;
            while ((size == null || size-- > 0) && !(r = await it.next()).done) {
                if (!sink.push(r.value)) { return; }
            }
            r && r.done && await end(sink);
        }
        async function close(signal: 'throw' | 'return', value?: any) {
            if (it && typeof it[signal] === 'function') {
                await it[signal]!(value);
            }
        }
        async function end(sink: ReadableNodeStream) {
            sink.push(null);
            await close('return');
        }
    }
}
