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

import { Data } from '../../src/data';
import { IntVector } from "../../src/vector";

import { Int8,       Int16,       Int32,       Int64 } from '../../src/type';
import { Int8Vector, Int16Vector, Int32Vector, Int64Vector } from '../../src/vector';

// import { Uint8,       Uint16,       Uint32,       Uint64 } from '../../src/type';
// import { UInt8Vector, UInt16Vector, UInt32Vector, UInt64Vector } from '../../src/vector';

describe('Int8Vector', () => {

    const length = 10;

    test('can create an Int8Vector from Data instance', () => {
        const intType = new Int8();
        const intVals = Int8Array.from({ length }, (_, b) => b);
        const intVector: Int8Vector = IntVector.new(Data.Int(intType, 0, length, 0, null, intVals));
        for (let i = 0; i < length; i++) {
            expect(intVector.get(i)).toEqual(i);
        }
    })

    test('can create an Int16Vector from Data instance', () => {
        const intType = new Int16();
        const intVals = Int16Array.from({ length }, (_, b) => b);
        const intVector: Int16Vector = IntVector.new(Data.Int(intType, 0, length, 0, null, intVals));
        for (let i = 0; i < length; i++) {
            expect(intVector.get(i)).toEqual(i);
        }

    })
})
