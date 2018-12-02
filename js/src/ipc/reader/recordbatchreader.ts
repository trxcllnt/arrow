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
import { MessageReader } from '../message/messagereader';
import { VectorLoader } from '../../visitor/vectorloader';
import { AbstractRecordBatchReader } from './abstractrecordbatchreader';
import { ReadableDOMStream, ReadableNodeStream } from '../../io/interfaces';

export abstract class RecordBatchReader<T extends { [key: string]: DataType } = any>
       extends AbstractRecordBatchReader<MessageReader>
       implements IterableIterator<RecordBatch<T>> {

    public [Symbol.iterator](): IterableIterator<RecordBatch> { return this as any; }
    public open() { this.readSchema(); return this; }
    public readSchema() {
        return this._schema || (this._schema = this.source.readSchema());
    }
    public readMessage<T extends MessageHeader>(type?: T | null): Message<T> | null {
        const message = this.source.readMessage(type);
        if (message && message.isDictionaryBatch()) {
            this._dictionaryIndex++;
        } else if (message && message.isRecordBatch()) {
            this._recordBatchIndex++;
        }
        return message;
    }
    public next(): IteratorResult<RecordBatch<T>> {
        let message: Message | null;
        let schema = this.readSchema();
        let dictionaries = this.dictionaries;
        let header: metadata.RecordBatch | metadata.DictionaryBatch;
        while (message = this.readMessage()) {
            if (message.isSchema()) {
                this._reset(message.header()! as Schema<T>);
                break;
            } else if (message.isRecordBatch() && (header = message.header()!)) {
                return { done: false, value: this._loadRecordBatch(schema, message.bodyLength, header) };
            } else if (message.isDictionaryBatch() && (header = message.header()!)) {
                dictionaries.set(header.id, this._loadDictionaryBatch(schema, message.bodyLength, header));
            }
        }
        return ITERATOR_DONE;
    }
    protected _loadRecordBatch(schema: Schema<T>, bodyLength: number, header: metadata.RecordBatch) {
        return new RecordBatch<T>(schema, header.length, this._vectorLoader(bodyLength, header).visitMany(schema.fields));
    }
    protected _loadDictionaryBatch(schema: Schema<T>, bodyLength: number, header: metadata.DictionaryBatch): Vector {
        const { dictionaries } = this;
        const { id, isDelta } = header;
        if (isDelta || !dictionaries.get(id)) {
            const type = schema.dictionaries.get(id)!;
            const loader = this._vectorLoader(bodyLength, header);
            const vector = (isDelta ? dictionaries.get(id)!.concat(
                Vector.new(loader.visit(type.dictionary))) :
                Vector.new(loader.visit(type.dictionary))) as Vector;
            return (type.dictionaryVector = vector);
        }
        return dictionaries.get(id)!;
    }
    protected _vectorLoader(bodyLength: number, metadata: metadata.RecordBatch | metadata.DictionaryBatch) {
        return new VectorLoader(this.source.readMessageBody(bodyLength), metadata.nodes, metadata.buffers);
    }
    public asReadableDOMStream() {
        let self = this, it: Iterator<RecordBatch>;
        return new ReadableDOMStream<RecordBatch>({
            cancel: close.bind(null, 'return'),
            start() { it = self[Symbol.iterator](); },
            pull(controller) {
                try {
                    let size = controller.desiredSize;
                    let r: IteratorResult<RecordBatch> | null = null;
                    while ((size == null || size-- > 0) && !(r = it.next()).done) {
                        controller.enqueue(r.value);
                    }
                    r && r.done && [close('return'), controller.close()];
                } catch (e) {
                    close('throw', e);
                    controller.error(e);
                }
            }
        });
        function close(signal: 'throw' | 'return', value?: any) {
            if (it && typeof it[signal] === 'function') {
                it[signal]!(value);
            }
        }
    }
    public asReadableNodeStream() {
        let self = this, it: Iterator<RecordBatch>;
        return new ReadableNodeStream({
            objectMode: true,
            read(size: number) {
                (it || (it = self[Symbol.iterator]())) && read(this, size);
            },
            destroy(e: Error | null, cb: (e: Error | null) => void) {
                const signal = e == null ? 'return' : 'throw';
                try { close(signal, e); } catch (err) {
                    return cb && Promise.resolve(err).then(cb);
                }
                return cb && Promise.resolve(null).then(cb);
            }
        });
        function read(sink: ReadableNodeStream, size: number) {
            let r: IteratorResult<RecordBatch> | null = null;
            while ((size == null || size-- > 0) && !(r = it.next()).done) {
                if (!sink.push(r.value)) { return; }
            }
            r && r.done && end(sink);
        }
        function close(signal: 'throw' | 'return', value?: any) {
            if (it && typeof it[signal] === 'function') {
                it[signal]!(value);
            }
        }
        function end(sink: ReadableNodeStream) {
            sink.push(null);
            close('return');
        }
    }
}
