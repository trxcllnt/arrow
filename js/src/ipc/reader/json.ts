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

import { ArrowJSON } from '../../io';
import { DataType } from '../../type';
import { RecordBatchReader } from './base';
import { JSONMessageReader } from './message';
import * as metadata from '../metadata/message';
import { JSONVectorLoader } from '../../visitor/vectorloader';

export class RecordBatchJSONReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {
    // @ts-ignore
    protected source: JSONMessageReader;
    constructor(json: ArrowJSON) { super(new JSONMessageReader(json)); }
    protected _vectorLoader(bodyLength: number, metadata: metadata.RecordBatch | metadata.DictionaryBatch) {
        return new JSONVectorLoader(this.source.readMessageBody(bodyLength), metadata.nodes, metadata.buffers);
    }
}
