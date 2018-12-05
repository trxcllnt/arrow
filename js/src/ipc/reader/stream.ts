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

import { DataType } from '../../type';
import { ArrowStream, AsyncArrowStream } from '../../io';
import { ByteStream, AsyncByteStream } from '../../io/stream';
import { MessageReader, AsyncMessageReader } from './message';
import { RecordBatchReader, AsyncRecordBatchReader } from './base';
import { ReadableDOMStream, FileHandle } from '../../io/interfaces';

export class RecordBatchStreamReader<T extends { [key: string]: DataType } = any> extends RecordBatchReader<T> {
    constructor(source: ArrowStream | ByteStream | ArrayBufferView | Iterable<ArrayBufferView>) {
        super(new MessageReader(source));
    }
}

export class AsyncRecordBatchStreamReader<T extends { [key: string]: DataType } = any> extends AsyncRecordBatchReader<T> {
    constructor(source: AsyncArrowStream | AsyncByteStream | NodeJS.ReadableStream | ReadableDOMStream<ArrayBufferView> | AsyncIterable<ArrayBufferView>);
    constructor(source: FileHandle, byteLength?: number);
    constructor(source: any, byteLength?: number) {
        super(new AsyncMessageReader(source, byteLength));
    }
}
