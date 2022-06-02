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

import { Field, Float32, makeData, Struct, StructRow, StructRowProxy } from 'apache-arrow';

function makeStructRow() {
    const struct = makeData({
        length: 3,
        type: new Struct<{ foo: Float32 }>([
            new Field('foo', new Float32())
        ]),
        children: [
            makeData({
                type: new Float32(),
                data: new Float32Array([0, 1, 2]),
            })
        ]
    });
    const proto = new StructRow(struct.type);
    const row = Object.create(proto);
    row[Symbol.for('parent')] = struct;
    row[Symbol.for('rowIndex')] = 0;
    return row as StructRowProxy<{ foo: Float32 }>;
}

describe('StructRow', () => {
    test('constructor is StructRow', () => {
        const row = makeStructRow();
        expect(row.constructor).toBe(StructRow);
    });

    test('Can set existing property', () => {
        const row = makeStructRow();
        row.foo = 42;
        expect(row.foo).toBe(42);
    });

    test('Can set arbitrary symbols', () => {
        const row = makeStructRow();
        const s = Symbol.for('mySymbol');
        row[s] = 42;
        expect(row[s]).toBe(42);
    });
});
