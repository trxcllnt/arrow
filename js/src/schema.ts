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

import * as Schema_ from './fb/Schema';
import * as Message_ from './fb/Message';
import { flatbuffers } from 'flatbuffers';

export import Long = flatbuffers.Long;
import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;
import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;

import { Vector } from './vector';
import { DataType, Dictionary, Int } from './type';

export class Schema {
    public static from(vectors: Vector[]) {
        return new Schema(vectors.map((v, i) => new Field('' + i, v.type)));
    }
    // @ts-ignore
    protected _bodyLength: number;
    // @ts-ignore
    protected _headerType: MessageHeader;
    public readonly fields: Field[];
    public readonly version: MetadataVersion;
    public readonly metadata: Map<string, string>;
    public readonly dictionaries: Map<number, DataType>;
    constructor(fields: Field[],
                metadata?: Map<string, string>,
                version: MetadataVersion = MetadataVersion.V4,
                dictionaries: Map<number, DataType> = new Map()) {
        this.fields = fields;
        this.version = version;
        this.dictionaries = dictionaries;
        this.metadata = metadata || Schema.prototype.metadata;
    }
    public get bodyLength() { return this._bodyLength; }
    public get headerType() { return this._headerType; }
    public select(...columnNames: string[]): Schema {
        const names = columnNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
        const fields = this.fields.filter((f) => names[f.name]);
        const dictionaries = (fields.filter((f) => DataType.isDictionary(f.type)) as Field<Dictionary<any>>[])
            .reduce((d, f) =>  d.set(f.type.id, this.dictionaries.get(f.type.id)!), new Map<number, DataType>());
        return new Schema(fields, this.metadata, this.version, dictionaries);
    }
    public static [Symbol.toStringTag] = ((prototype: Schema) => {
        prototype._bodyLength = 0;
        prototype._headerType = MessageHeader.Schema;
        (prototype as any).metadata = Object.freeze(new Map());
        return 'Schema';
    })(Schema.prototype);
}

export class Field<T extends DataType = DataType> {
    public readonly type: T;
    public readonly name: string;
    public readonly nullable: true | false;
    public readonly metadata?: Map<string, string> | null;
    constructor(name: string, type: T, nullable: true | false = false, metadata?: Map<string, string> | null) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.metadata = metadata;
    }
    public toString() { return `${this.name}: ${this.type}`; }
    public get typeId(): T['TType'] { return this.type.TType; }
    public get [Symbol.toStringTag](): string { return 'Field'; }
    public get indices(): T | Int<any> {
        return DataType.isDictionary(this.type) ? this.type.indices : this.type;
    }
}
