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

import { Readable } from 'stream';
import { toUint8Array } from '../../../src/util/buffer';
import { ReadableDOMStream } from '../../../src/io/interfaces';

export function nodeToDOMStream(stream: NodeJS.ReadableStream, opts: any = {}) {
    stream = new Readable().wrap(stream);
    return new ReadableDOMStream({
        ...opts,
        start(controller) {
            stream.pause();
            stream.on('data', (chunk) => {
                // We have to copy the chunk bytes here because the whatwg ReadableByteStream reference
                // implementation redefines the `byteLength` property of the underlying ArrayBuffers that
                // pass through it, to mimic the (as yet specified) ArrayBuffer "transfer" semantics.
                // Node allocates ArrayBuffers in 64k chunks, and shares them between unrelated active
                // file streams.
                // When the ReadableByteStream sets the ArrayBuffer's `byteLength` to 0, it isn't just
                // setting this chunk's ArrayBuffer byteLength to zero, but rather the ArrayBuffer for
                // all the currently active file streams. See the code here for more details:
                // https://github.com/whatwg/files/blob/3197c7e69456eda08377c18c78ffc99831b5a35f/reference-implementation/lib/helpers.js#L126
                controller.enqueue(toUint8Array(chunk).slice());
                stream.pause();
            });
            stream.on('end', () => controller.close());
            stream.on('error', e => controller.error(e));
        },
        pull() { stream.resume(); },
        cancel(reason) {
            stream.pause();
            if (typeof (stream as any).cancel === 'function') {
                return (stream as any).cancel(reason);
            } else if (typeof (stream as any).destroy === 'function') {
                return (stream as any).destroy(reason);
            }
        }
    })
}
