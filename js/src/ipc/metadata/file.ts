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

import * as File_ from '../../fb/File';
import { flatbuffers } from 'flatbuffers';

import Long = flatbuffers.Long;
import ByteBuffer = flatbuffers.ByteBuffer;
import _Block = File_.org.apache.arrow.flatbuf.Block;
import _Footer = File_.org.apache.arrow.flatbuf.Footer;

import './schema';
import { Schema } from '../../schema';
import { toUint8Array } from '../../util/buffer';
import { ArrayBufferViewInput } from '../../util/buffer';
import { MetadataVersion } from '../../enum';

export class Footer {

    static decode(buf: ArrayBufferViewInput) {
        buf = new ByteBuffer(toUint8Array(buf));
        const footer = _Footer.getRootAsFooter(buf);
        const schema = Schema.decode(footer.schema()!, new Map());
        return new OffHeapFooter(schema, footer) as Footer;
    }

    // @ts-ignore
    protected _recordBatches: FileBlock[];
    // @ts-ignore
    protected _dictionaryBatches: FileBlock[];
    public get numRecordBatches() { return this._recordBatches.length; }
    public get numDictionaries() { return this._dictionaryBatches.length; }

    constructor(public schema: Schema,
                public version: MetadataVersion = MetadataVersion.V4,
                recordBatches?: FileBlock[], dictionaryBatches?: FileBlock[]) {
        recordBatches && (this._recordBatches = recordBatches);
        dictionaryBatches && (this._recordBatches = dictionaryBatches);
    }

    public *recordBatches(): Iterable<FileBlock> {
        for (let block, i = -1, n = this.numRecordBatches; ++i < n;) {
            if (block = this.getRecordBatch(i)) { yield block; }
        }
    }

    public *dictionaryBatches(): Iterable<FileBlock> {
        for (let block, i = -1, n = this.numDictionaries; ++i < n;) {
            if (block = this.getDictionaryBatch(i)) { yield block; }
        }
    }

    public getRecordBatch(index: number) {
        return index >= 0
            && index < this.numRecordBatches
            && this._recordBatches[index] || null; 
    }

    public getDictionaryBatch(index: number) {
        return index >= 0
            && index < this.numDictionaries
            && this._dictionaryBatches[index] || null; 
    }
}

class OffHeapFooter extends Footer {

    public get numRecordBatches() { return this._footer.recordBatchesLength(); }
    public get numDictionaries() { return this._footer.dictionariesLength(); }

    constructor(schema: Schema, protected _footer: _Footer) {
        super(schema, _footer.version());
    }

    public getRecordBatch(index: number) {
        if (index >= 0 && index < this.numRecordBatches) {
            const fileBlock = this._footer.recordBatches(index);
            if (fileBlock) { return FileBlock.decode(fileBlock); }
        }
        return null; 
    }

    public getDictionaryBatch(index: number) {
        if (index >= 0 && index < this.numDictionaries) {
            const fileBlock = this._footer.dictionaries(index);
            if (fileBlock) { return FileBlock.decode(fileBlock); }
        }
        return null; 
    }
}

export class FileBlock {

    static decode(block: _Block) {
        return new FileBlock(block.metaDataLength(), block.bodyLength(), block.offset());
    }
    
    public offset: number;
    public bodyLength: number;
    public metaDataLength: number;

    constructor(metaDataLength: number, bodyLength: Long | number, offset: Long | number) {
        this.metaDataLength = metaDataLength;
        this.offset = typeof offset === 'number' ? offset : offset.low;
        this.bodyLength = typeof bodyLength === 'number' ? bodyLength : bodyLength.low;
    }
}
