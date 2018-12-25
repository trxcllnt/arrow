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
import { RecordBatch } from '../../recordbatch';
import { AsyncByteStream } from '../../io/stream';
import { RecordBatchWriter } from '../../ipc/writer';
import { protectArrayBufferFromWhatwgRefImpl } from './hack';

export function recordBatchWriterThroughDOMStream<T extends { [key: string]: DataType } = any>(
    this: typeof RecordBatchWriter,
    writableStrategy?: QueuingStrategy<RecordBatch<T>>,
    readableStrategy?: { highWaterMark?: number, size?: any }
) {

    const writer = new this<T>();
    const reader = new AsyncByteStream(writer);
    const readable = new ReadableStream({
        type: 'bytes',
        async cancel() { await reader.cancel(); },
        async pull(controller) { await next(controller); },
        async start(controller) { await next(controller); },
    }, readableStrategy);

    return { writable: new WritableStream(writer, writableStrategy), readable };

    async function next(controller: ReadableStreamDefaultController<Uint8Array>) {
        let buf: Uint8Array | null = null;
        let size = controller.desiredSize;
        while (buf = await reader.read(size || null)) {
            controller.enqueue(protectArrayBufferFromWhatwgRefImpl(buf));
            if (size != null && (size -= buf.byteLength) <= 0) { return; }
        }
        controller.close();
    }
}
