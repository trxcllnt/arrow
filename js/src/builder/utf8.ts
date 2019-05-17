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

import { Utf8 } from '../type';
import { FlatListBuilder } from './base';
import { encodeUtf8 } from '../util/utf8';

export interface Utf8Builder<TNull = any> extends FlatListBuilder<Utf8, TNull> {
    nullBitmap: Uint8Array;
    valueOffsets: Int32Array;
    values: Uint8Array;
}

export class Utf8Builder<TNull = any> extends FlatListBuilder<Utf8, TNull> {
    public writeValue(value: string, index = this.length) {
        return super.writeValue(encodeUtf8(value), index);
    }
}
