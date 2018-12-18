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

import { Field } from './schema';
import { Vector } from './vector';
import { DataType } from './type';
import { ChunkedVector } from './vector/chunked';

export class Column<T extends DataType = any> extends ChunkedVector<T> {

    constructor(field: Field<T>, vectors: Vector<T>[] = [], offsets?: Uint32Array) {
        super(field.type, ChunkedVector.flatten(...vectors), offsets);
        this._field = field;
    }

    protected _children?: Column[];
    protected _field: Field<T>;

    public get field() { return this._field; }
    public get name() { return this.field.name; }

    public slice(begin?: number, end?: number): Column<T> {
        return new Column(this.field, super.slice(begin, end).chunks);
    }

    public getChildAt<R extends DataType = any>(index: number): Column<R> | null {

        if (index < 0 || index >= this.numChildren) { return null; }

        let columns = this._children || (this._children = []);
        let column: Column<R>, field: Field<R>, chunks: Vector<R>[];

        if (column = columns[index]) { return column; }
        if (field = ((this.type.children || [])[index] as Field<R>)) {
            chunks = this.chunks
                .map((vector) => vector.getChildAt<R>(index))
                .filter((vec): vec is Vector<R> => vec != null);
            if (chunks.length > 0) {
                return (columns[index] = new Column<R>(field, chunks));
            }
        }

        return null;
    }
}
