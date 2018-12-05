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
import { Footer } from '../metadata/file';
import { MessageHeader } from '../../enum';
import { Message } from '../metadata/message';
import { FileHandle } from '../../io/interfaces';
import { ArrowFile, AsyncArrowFile } from '../../io';
import { ArrayBufferViewInput } from '../../util/buffer';
import { RecordBatchReader, AsyncRecordBatchReader } from './base';
import { MessageReader, AsyncMessageReader, magicAndPadding } from './message';

export class RecordBatchFileReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {

    protected file: ArrowFile;
    public get footer() { return this._footer; }
    public get numDictionaries() { return this._footer.numDictionaries; }
    public get numRecordBatches() { return this._footer.numRecordBatches; }

    constructor(source: ArrowFile);
    constructor(source: ArrayBufferViewInput);
    constructor(source: ArrowFile | ArrayBufferViewInput) {
        const file = source instanceof ArrowFile ? source : new ArrowFile(source);
        super(new MessageReader(file));
        this.file = file;
    }

    public readSchema() {
        return this._schema || (this._schema = this.readFooter().schema);
    }

    public readMessage<T extends MessageHeader>(type?: T | null): Message<T> | null {
        const block =
              (this._dictionaryIndex < this.numDictionaries) ? this._footer.getDictionaryBatch(this._dictionaryIndex)
            : (this._recordBatchIndex < this.numRecordBatches) ? this._footer.getRecordBatch(this._recordBatchIndex)
            : null;
        return (block && this.file.seek(block.offset)) ? super.readMessage(type) : null;
    }

    public readRecordBatch(index: number) {
        const block = this._footer.getRecordBatch(index);
        if (block && this.file.seek(block.offset)) {
            const message = this.readMessage(MessageHeader.RecordBatch);
            if (message && message.isRecordBatch()) {
                return this._loadRecordBatch(this.schema, message.bodyLength, message.header()!);
            }
        }
        return null;
    }

    public readDictionaryBatch(index: number) {
        const block = this._footer.getDictionaryBatch(index);
        if (block && this.file.seek(block.offset)) {
            const message = this.readMessage(MessageHeader.DictionaryBatch);
            if (message && message.isDictionaryBatch()) {
                return this._loadDictionaryBatch(this.schema, message.bodyLength, message.header()!);
            }
        }
        return null;
    }

    public readFooter() {
        if (!this._footer) {
            const { file } = this;
            const size = file.size;
            const offset = size - magicAndPadding;
            const length = file.readInt32(offset);
            const buffer = file.readAt(offset - length, length);
            const footer = this._footer = Footer.decode(buffer);
            for (const block of footer.dictionaryBatches()) {
                block && this.readDictionaryBatch(this._dictionaryIndex);
            }
        }
        return this._footer;
    }
}

export class AsyncRecordBatchFileReader<T extends { [key: string]: DataType } = any> extends AsyncRecordBatchReader<T> {

    protected file: AsyncArrowFile;
    public get footer() { return this._footer; }
    public get numDictionaries() { return this._footer.numDictionaries; }
    public get numRecordBatches() { return this._footer.numRecordBatches; }

    constructor(source: AsyncArrowFile);
    constructor(source: FileHandle, byteLength: number);
    constructor(source: AsyncArrowFile | FileHandle, byteLength?: number) {
        const file = source instanceof AsyncArrowFile ? source : new AsyncArrowFile(source, byteLength!);
        super(new AsyncMessageReader(file));
        this.file = file;
    }

    public async readSchema() {
        return this._schema || (this._schema = (await this.readFooter()).schema);
    }

    public async readMessage<T extends MessageHeader>(type?: T | null): Promise<Message<T> | null> {
        const block =
              (this._dictionaryIndex < this.numDictionaries) ? this._footer.getDictionaryBatch(this._dictionaryIndex)
            : (this._recordBatchIndex < this.numRecordBatches) ? this._footer.getRecordBatch(this._recordBatchIndex)
            : null;
        return (block && (await this.file.seek(block.offset))) ? await super.readMessage(type) : null;
    }

    public async readRecordBatch(index: number) {
        const block = this._footer.getRecordBatch(index);
        if (block && (await this.file.seek(block.offset))) {
            const message = await super.readMessage(MessageHeader.RecordBatch);
            if (message && message.isRecordBatch()) {
                return await this._loadRecordBatch(this.schema, message.bodyLength, message.header()!);
            }
        }
        return null;
    }

    public async readDictionaryBatch(index: number) {
        const block = this._footer.getDictionaryBatch(index);
        if (block && (await this.file.seek(block.offset))) {
            const message = await super.readMessage(MessageHeader.DictionaryBatch);
            if (message && message.isDictionaryBatch()) {
                return await this._loadDictionaryBatch(this.schema, message.bodyLength, message.header()!);
            }
        }
        return null;
    }

    public async readFooter() {
        if (!this._footer) {
            const { file } = this;
            const offset = file.size - magicAndPadding;
            const length = await file.readInt32(offset);
            const buffer = await file.readAt(offset - length, length);
            const footer = this._footer = Footer.decode(buffer);
            for (const block of footer.dictionaryBatches()) {
                block && (await this.readDictionaryBatch(this._dictionaryIndex));
            }
        }
        return this._footer;
    }
}
