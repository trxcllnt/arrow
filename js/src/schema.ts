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

import { Vector } from './vector';
import { DataType, Dictionary } from './type';

function generateDictionaryMap(fields: Field[]) {
    return fields
        .filter((f): f is Field<Dictionary> => DataType.isDictionary(f.type))
        .reduce((dictionaries, f) => {
            if (dictionaries.has(f.type.id)) {
                throw new Error(`Cannot create Schema containing two dictionaries with the same ID`);
            }
            return dictionaries.set((f.type as Dictionary).id, f.type.dictionary);
        }, new Map());
}

export class Schema<T extends { [key: string]: DataType } = any> {
    public static from<T extends { [key: string]: DataType } = any>(vectors: Vector<T[keyof T]>[]) {
        return new Schema<T>(vectors.map((v, i) => new Field('' + i, v.type)));
    }
    public readonly fields: Field[];
    public readonly metadata: Map<string, string>;
    public readonly dictionaries: Map<number, Dictionary>;
    constructor(fields: Field[],
                metadata?: Map<string, string>,
                dictionaries?: Map<number, Dictionary>) {
        this.fields = fields;
        this.metadata = metadata || Schema.prototype.metadata;
        this.dictionaries = dictionaries || generateDictionaryMap(fields);
    }
    public select<K extends keyof T = any>(...columnNames: K[]) {
        const names = columnNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
        const fields = this.fields.filter((f) => names[f.name]);
        const dictionaries = (fields.filter((f) => DataType.isDictionary(f.type)) as Field<Dictionary<any>>[])
            .reduce((d, f) =>  d.set(f.type.id, this.dictionaries.get(f.type.id)!), new Map<number, Dictionary>());
        return new Schema<{ [P in K]: T[P] }>(fields, this.metadata, dictionaries);
    }
    public static [Symbol.toStringTag] = ((prototype: Schema) => {
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
    public get indices() {
        return DataType.isDictionary(this.type) ? this.type.indices : this.type;
    }
}
