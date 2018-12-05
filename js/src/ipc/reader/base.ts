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
import { MessageReader, AsyncMessageReader } from './message';
import { RecordBatchReader as AbstractRecordBatchReader } from '../reader';

export abstract class RecordBatchReader<T extends { [key: string]: DataType } = any> extends AbstractRecordBatchReader<T> {

    constructor(protected source: MessageReader, dictionaries = new Map<number, Vector>()) {
        super(source, dictionaries);
    }

    public open(autoClose = this._autoClose) {
        this._autoClose = autoClose;
        this.readSchema();
        return this;
    }
    public close() { this.reset(null).source.return(); return this; }

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

    public [Symbol.iterator](): IterableIterator<RecordBatch<T>> {
        return this as IterableIterator<RecordBatch<T>>;
    }
    public throw(value?: any) {
        return this._autoClose ? this.reset().source.throw(value) : ITERATOR_DONE;
    }
    public return(value?: any) {
        return this._autoClose ? this.reset().source.return(value) : ITERATOR_DONE;
    }
    public next(): IteratorResult<RecordBatch<T>> {
        let message: Message | null;
        let schema = this.readSchema();
        let dictionaries = this.dictionaries;
        let header: metadata.RecordBatch | metadata.DictionaryBatch;
        while (message = this.readMessage()) {
            if (message.isSchema()) {
                return this.reset(message.header()! as Schema<T>).return();
            } else if (message.isRecordBatch() && (header = message.header()!)) {
                return { done: false, value: this._loadRecordBatch(schema, message.bodyLength, header) };
            } else if (message.isDictionaryBatch() && (header = message.header()!)) {
                dictionaries.set(header.id, this._loadDictionaryBatch(schema, message.bodyLength, header));
            }
        }
        return this.reset(null).return();
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
}

export abstract class AsyncRecordBatchReader<T extends { [key: string]: DataType } = any> extends AbstractRecordBatchReader<T> {

    constructor(protected source: AsyncMessageReader, dictionaries = new Map<number, Vector>()) {
        super(source, dictionaries);
    }

    public async open(autoClose = this._autoClose) {
        this._autoClose = autoClose;
        await this.readSchema();
        return this;
    }
    public async close() { await this.reset(null).source.return(); return this; }

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

    public [Symbol.asyncIterator](): AsyncIterableIterator<RecordBatch<T>> {
        return this as AsyncIterableIterator<RecordBatch<T>>;
    }
    public async throw(value?: any) {
        return this._autoClose ? await this.reset().source.throw(value) : ITERATOR_DONE;
    }
    public async return(value?: any) {
        return this._autoClose ? await this.reset().source.return(value) : ITERATOR_DONE;
    }
    public async next() {
        let message: Message | null;
        let schema = await this.readSchema();
        let dictionaries = this.dictionaries;
        let header: metadata.RecordBatch | metadata.DictionaryBatch;
        while (message = await this.readMessage()) {
            if (message.isSchema()) {
                return await this.reset(message.header()! as Schema<T>).return();
            } else if (message.isRecordBatch() && (header = message.header()!)) {
                return { done: false, value: await this._loadRecordBatch(schema, message.bodyLength, header) };
            } else if (message.isDictionaryBatch() && (header = message.header()!)) {
                dictionaries.set(header.id, await this._loadDictionaryBatch(schema, message.bodyLength, header));
            }
        }
        return await this.reset(null).return();
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
}
