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

import { Data } from '../data.js';
import { Field } from '../schema.js';
import { Struct, TypeMap } from '../type.js';
import { valueToString } from '../util/pretty.js';
import { instance as getVisitor } from '../visitor/get.js';
import { instance as setVisitor } from '../visitor/set.js';

/** @ignore */ export const kParent = Symbol.for('parent');
/** @ignore */ export const kRowIndex = Symbol.for('rowIndex');
/** @ignore */ const kRowCache = Symbol.for('rowCache');

export type StructRowProxy<T extends TypeMap = any> = StructRow<T> & {
    [P in string & keyof T]: T[P]['TValue'];
} & {
    [key: number | symbol]: any;
};

export class StructRow<T extends TypeMap = any> {

    declare public [kRowIndex]: number;
    declare public [kRowCache]: Array<any>;
    declare public [kParent]: Data<Struct<T>>;

    [key: string | number | symbol]: any;

    constructor(type: Struct<T>) {
        Object.defineProperties(this, type.children.reduce((descriptors, { name }, colIndex) => {
            descriptors[name] = {
                enumerable: true,
                configurable: true,
                get: bindGetter(colIndex),
                set: bindSetter(colIndex),
            };
            return descriptors;
        }, Object.create(null)));
    }

    public toArray() { return Object.values(toJSON<T>(this)); }
    public toJSON() { return toJSON<T>(this); }

    public toString() {
        return `{${[...this].map(([key, val]) =>
            `${valueToString(key)}: ${valueToString(val)}`
        ).join(', ')
            }}`;
    }

    public [Symbol.for('nodejs.util.inspect.custom')]() {
        return this.toString();
    }

    [Symbol.iterator](): IterableIterator<[
        string & keyof T, { [P in keyof T]: T[P]['TValue'] | null }[string & keyof T]
    ]> {
        return new StructRowIterator(this[kParent], this[kRowIndex]);
    }
}

function toJSON<T extends TypeMap = any>(row: StructRow<T>) {
    const i = row[kRowIndex];
    const parent = row[kParent];
    const keys = parent.type.children;
    const json = {} as { [P in string & keyof T]: T[P]['TValue'] };
    for (let j = -1, n = keys.length; ++j < n;) {
        json[keys[j].name as string & keyof T] = getVisitor.visit(parent.children[j], i);
    }
    return json;
}

class StructRowIterator<T extends TypeMap = any>
    implements IterableIterator<[
        keyof T, { [P in keyof T]: T[P]['TValue'] | null }[keyof T]
    ]> {

    declare private rowIndex: number;
    declare private childIndex: number;
    declare private numChildren: number;
    declare private children: Data<any>[];
    declare private childFields: Field<T[keyof T]>[];

    constructor(data: Data<Struct<T>>, rowIndex: number) {
        this.childIndex = 0;
        this.children = data.children;
        this.rowIndex = rowIndex;
        this.childFields = data.type.children;
        this.numChildren = this.childFields.length;
    }

    [Symbol.iterator]() { return this; }

    next() {
        const i = this.childIndex;
        if (i < this.numChildren) {
            this.childIndex = i + 1;
            return {
                done: false,
                value: [
                    this.childFields[i].name,
                    getVisitor.visit(this.children[i], this.rowIndex)
                ]
            } as IteratorYieldResult<[any, any]>;
        }
        return { done: true, value: null } as IteratorReturnResult<null>;
    }
}

function bindGetter(colIndex: number) {
    return function get(this: StructRow<any>) {
        const cache = this[kRowCache] || (this[kRowCache] = new Array(this[kParent].children.length));
        let value = cache[colIndex];
        if (value === undefined) {
            const child = this[kParent].children[colIndex];
            value = child ? getVisitor.visit(child, this[kRowIndex]) : null;
            // Cache key/val lookups
            cache[colIndex] = value;
        }
        return value;
    };
}

function bindSetter(colIndex: number) {
    return function set(this: StructRow<any>, value: any) {
        const cache = this[kRowCache] || (this[kRowCache] = new Array(this[kParent].children.length));
        const child = this[kParent].children[colIndex];
        // Cache key/val lookups
        cache[colIndex] = value;
        if (child) {
            setVisitor.visit(child, this[kRowIndex], value);
        }
    };
}

Object.defineProperties(StructRow.prototype, {
    [Symbol.toStringTag]: { enumerable: false, configurable: false, value: 'Row' },
    [kParent]: { writable: true, enumerable: false, configurable: false, value: null },
    [kRowIndex]: { writable: true, enumerable: false, configurable: false, value: -1 },
    [kRowCache]: { writable: true, enumerable: false, configurable: false, value: null },
});
