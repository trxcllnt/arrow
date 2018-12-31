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

const kIsFakeBuffer = Symbol.for('isFakeBuffer');

// The Whatwg ReadableByteStream reference implementation[1] copies the
// underlying ArrayBuffer for any TypedArray that passes through it and
// redefines the original's byteLength to be 0, in order to mimic the
// unfinished ArrayBuffer "transfer" spec [2].
// 
// This is problematic in node, where a number of APIs (like fs.ReadStream)
// internally allocate and share ArrayBuffers between unrelated operations.
// It's also problematic when using the reference implementation as a polyfill
// in the browser, since it leads to the same bytes being copied at every link
// in a bytestream pipeline.
// 
// They do this because there are some web-platform tests that check whether
// byteLength has been set to zero to infer whether the buffer has been
// "transferred". We don't need to care about these tests in production, and
// we also wish to _not_ copy bytes as they pass through a stream, so this
// function fakes out the reference implementation to work around both these
// issues.
// 
// 1. https://github.com/whatwg/streams/blob/0ebe4b042e467d9876d80ae045de3843092ad797/reference-implementation/lib/helpers.js#L126
// 2. https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/ArrayBuffer/transfer

/** @ignore */
export function protectArrayBufferFromWhatwgRefImpl(value: Uint8Array) {
    const real = value.buffer;
    if (!(real as any)[kIsFakeBuffer]) {
        const fake = Object.create(real);
        Object.defineProperty(fake, kIsFakeBuffer, { value: true });
        Object.defineProperty(fake, 'slice', { value: () => real });
        Object.defineProperty(value, 'buffer', {     value: fake });
    }
    return value;
}
