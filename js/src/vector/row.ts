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

import { Field } from '../schema';
import { MapVector } from '../vector/map';
import { DataType, RowLike } from '../type';
import { valueToString } from '../util/pretty';
import { StructVector } from '../vector/struct';

/** @ignore */ export const kLength = Symbol.for('length');
/** @ignore */ export const kParent = Symbol.for('parent');
/** @ignore */ export const kRowIndex = Symbol.for('rowIndex');
/** @ignore */ const columnDescriptor = { enumerable: true, configurable: false, get: null as any };
/** @ignore */ const rowLengthDescriptor = { writable: false, enumerable: false, configurable: true, value: null as any };
/** @ignore */ const rowParentDescriptor = { writable: false, enumerable: false, configurable: false, value: null as any };

/** @ignore */
export class Row<T extends { [key: string]: DataType }> implements Iterable<T[keyof T]['TValue']> {
    [key: string]: T[keyof T]['TValue'];
    /** @nocollapse */
    public static new<T extends { [key: string]: DataType }>(parent: MapVector<T> | StructVector<T>, schemaOrFields: T | Field[], fieldsAreEnumerable = false): RowLike<T> & Row<T> {
        let schema: T, fields: Field[];
        if (Array.isArray(schemaOrFields)) {
            fields = schemaOrFields;
        } else {
            schema = schemaOrFields;
            fieldsAreEnumerable = true;
            fields = Object.keys(schema).map((x) => new Field(x, schema[x]));
        }
        return new Row<T>(parent, fields, fieldsAreEnumerable) as RowLike<T> & Row<T>;
    }
    // @ts-ignore
    private [kParent]: MapVector<T> | StructVector<T>;
    // @ts-ignore
    private [kLength]: number;
    // @ts-ignore
    private [kRowIndex]: number;
    private constructor(parent: MapVector<T> | StructVector<T>, fields: Field[], fieldsAreEnumerable: boolean) {
        rowParentDescriptor.value = parent;
        rowLengthDescriptor.value = fields.length;
        Object.defineProperty(this, kParent, rowParentDescriptor);
        Object.defineProperty(this, kLength, rowLengthDescriptor);
        fields.forEach((field, columnIndex) => {
            if (!this.hasOwnProperty(field.name)) {
                columnDescriptor.enumerable = fieldsAreEnumerable;
                columnDescriptor.get || (columnDescriptor.get = this._bindGetter(columnIndex));
                Object.defineProperty(this, field.name, columnDescriptor);
            }
            if (!this.hasOwnProperty(columnIndex)) {
                columnDescriptor.enumerable = !fieldsAreEnumerable;
                columnDescriptor.get || (columnDescriptor.get = this._bindGetter(columnIndex));
                Object.defineProperty(this, columnIndex, columnDescriptor);
            }
            columnDescriptor.get = null as any;
        });
        rowParentDescriptor.value = null as any;
        rowLengthDescriptor.value = null as any;
    }
    *[Symbol.iterator]() {
        for (let i = -1, n = this[kLength]; ++i < n;) {
            yield this[i];
        }
    }
    private _bindGetter(colIndex: number) {
        return function (this: Row<T>) {
            const child = this[kParent].getChildAt(colIndex);
            return child ? child.get(this[kRowIndex]) : null;
        };
    }
    public get<K extends keyof T>(key: K) { return (this as any)[key] as T[K]['TValue']; }
    public bind(rowIndex: number) {
        const bound = Object.create(this);
        bound[kRowIndex] = rowIndex;
        return bound as RowLike<T>;
    }
    public toJSON(): any {
        return DataType.isStruct(this.parent.type) ? [...this] :
            Object.getOwnPropertyNames(this).reduce((props: any, prop: string) => {
                return (props[prop] = (this as any)[prop]) && props || props;
            }, {});
    }
    public toString() {
        return DataType.isStruct(this.parent.type) ?
            [...this].map((x) => valueToString(x)).join(', ') :
            Object.getOwnPropertyNames(this).reduce((props: any, prop: string) => {
                return (props[prop] = valueToString((this as any)[prop])) && props || props;
            }, {});
    }
}

Object.defineProperty(Row.prototype, kRowIndex, { writable: true, enumerable: false, configurable: false, value: -1 });
