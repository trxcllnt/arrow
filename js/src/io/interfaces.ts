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

import { Readable, Writable } from 'stream';

export const ITERATOR_DONE: any = Object.freeze({ done: true, value: void (0) });

export type ArrowJSONInput = { schema: any; batches?: any[]; dictionaries?: any[]; };

export type FileHandle = import('fs').promises.FileHandle;
export type ReadableNodeStream = import('stream').Readable;
export type WritableNodeStream = import('stream').Writable;
export type ReadableNodeStreamOptions = import('stream').ReadableOptions;

export type ReadableDOMStream<R = any> = import('whatwg-streams').ReadableStream<R>;
export type WritableDOMStream<R = any> = import('whatwg-streams').WritableStream<R>;
export type ReadableDOMStreamOptions = { type?: 'bytes', autoAllocateChunkSize?: number };

export type PipeOptions = import('whatwg-streams').PipeOptions;
export type WritableReadablePair<
    T extends WritableDOMStream<any>,
    U extends ReadableDOMStream<any>
> = import('whatwg-streams').WritableReadablePair<T, U>;

export const ReadableNodeStream: typeof import('stream').Readable = Readable;
export const WritableNodeStream: typeof import('stream').Writable = Writable;
export const ReadableDOMStream: typeof import('whatwg-streams').ReadableStream = (<any> global).ReadableStream;
export const WritableDOMStream: typeof import('whatwg-streams').WritableStream = (<any> global).WritableStream;
