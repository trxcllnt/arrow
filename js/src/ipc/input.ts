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

import { flatbuffers } from 'flatbuffers';
import ByteBuffer = flatbuffers.ByteBuffer;

import { OptionallyAsync } from '../interfaces';
import { ReadableDOMStream } from '../io/interfaces';
import { ByteStream, AsyncByteStream } from '../io/stream';
import { toUint8Array, ArrayBufferViewInput } from '../util/buffer';
import { RandomAccessFile, AsyncRandomAccessFile } from '../io/file';

import { isArrowJSON, isFileHandle } from '../util/compat';
import { isPromise, isAsyncIterable } from '../util/compat';
import { isReadableNodeStream, isReadableDOMStream } from '../util/compat';

import { fromReadableDOMStream } from '../io/adapters/stream.dom';
import { fromReadableNodeStream } from '../io/adapters/stream.node';
import { fromIterable, fromAsyncIterable } from '../io/adapters/iterable';
import { checkForMagicArrowString, magicLength, magicX2AndPadding } from './magic';

type FileHandle = import('fs').promises.FileHandle;

export type ArrowIPCInput = object                             |
                            FileHandle                         |
                            ArrayBufferView                    |
                            NodeJS.ReadableStream              |
                            PromiseLike<FileHandle>            |
                            Iterable<ArrayBufferView>          |
                            PromiseLike<ArrayBufferView>       |
                            AsyncIterable<ArrayBufferView>     |
                            ReadableDOMStream<ArrayBufferView> ;

export function resolveInputFormat(source: ArrowIPCInput): InputResolver | AsyncInputResolver {
    if (                         isArrowJSON(source)) { return new JSONResolver(source)                                                         as      InputResolver; }
    if (                        isFileHandle(source)) { return new FileHandleResolver(source)                                                   as AsyncInputResolver; }
    if (                   isPromise<object>(source)) { return new PromiseResolver(source)                                                      as AsyncInputResolver; }
    if (               isPromise<FileHandle>(source)) { return new PromiseResolver(source)                                                      as AsyncInputResolver; }
    if (          isPromise<ArrayBufferView>(source)) { return new PromiseResolver(source)                                                      as AsyncInputResolver; }
    if (isReadableDOMStream<ArrayBufferView>(source)) { return new AsyncByteStreamResolver(new AsyncByteStream( fromReadableDOMStream(source))) as AsyncInputResolver; }
    if (                isReadableNodeStream(source)) { return new AsyncByteStreamResolver(new AsyncByteStream(fromReadableNodeStream(source))) as AsyncInputResolver; }
    if (    isAsyncIterable<ArrayBufferView>(source)) { return new AsyncByteStreamResolver(new AsyncByteStream(     fromAsyncIterable(source))) as AsyncInputResolver; }
                                                        return new ByteStreamResolver     (new      ByteStream(          fromIterable(source))) as      InputResolver;
}

export interface InputResolver {
     isSync(): this is InputResolver;
    isAsync(): this is AsyncInputResolver;
    resolve(): AsyncArrowInput | ArrowInput | null;
}

export interface AsyncInputResolver {
     isSync(): this is InputResolver;
    isAsync(): this is AsyncInputResolver;
    resolve(): Promise<AsyncArrowInput | ArrowInput | null>;
}

export interface ArrowInput {
      isFile(): this is ArrowFile;
      isJSON(): this is ArrowJSON;
    isStream(): this is ArrowStream;
}

export interface AsyncArrowInput {
      isFile(): this is AsyncArrowFile;
    isStream(): this is AsyncArrowStream;
}

export class ArrowFile extends RandomAccessFile<ByteBuffer> implements ArrowInput, OptionallyAsync<ArrowFile> {
    isJSON(): this is ArrowJSON { return false; }
    isFile(): this is ArrowFile { return true; }
  isStream(): this is ArrowStream { return false; }
    isSync(): this is ArrowFile { return true; }
   isAsync(): this is AsyncArrowFile { return false; }
  constructor(source: ArrayBufferViewInput) {
      super(toUint8Array(source));
  }
  next(size?: number) {
      const r = super.next(size) as IteratorResult<any>;
      !r.done && (r.value = new ByteBuffer(toUint8Array(r.value)));
      return r as IteratorResult<ByteBuffer>;
  }
}

export class ArrowJSON implements ArrowInput {
      isJSON(): this is ArrowJSON { return true; }
      isFile(): this is ArrowFile { return false; }
    isStream(): this is ArrowStream { return false; }
    constructor(public json: object) {}
}

export class AsyncArrowFile extends AsyncRandomAccessFile<ByteBuffer> implements AsyncArrowInput, OptionallyAsync<ArrowFile> {
      isJSON(): this is ArrowJSON { return false; }
      isFile(): this is AsyncArrowFile { return true; }
    isStream(): this is AsyncArrowStream { return false; }
      isSync(): this is ArrowFile { return false; }
     isAsync(): this is AsyncArrowFile { return true; }
    async next(size?: number) {
        const r = (await super.next(size)) as IteratorResult<any>;
        !r.done && (r.value = new ByteBuffer(toUint8Array(r.value)));
        return r as IteratorResult<ByteBuffer>;
    }
}

export class ArrowStream extends ByteStream<ByteBuffer> implements ArrowInput, OptionallyAsync<ArrowStream> {
      isJSON(): this is ArrowJSON { return false; }
      isFile(): this is ArrowFile { return false; }
    isStream(): this is ArrowStream { return true; }
      isSync(): this is ArrowStream { return true; }
     isAsync(): this is AsyncArrowStream { return false; }
    next(size?: number) {
        const r = super.next(size) as IteratorResult<any>;
        !r.done && (r.value = new ByteBuffer(toUint8Array(r.value)));
        return r as IteratorResult<ByteBuffer>;
    }
}

export class AsyncArrowStream extends AsyncByteStream<ByteBuffer> implements AsyncArrowInput, OptionallyAsync<ArrowStream> {
       isJSON(): this is ArrowJSON { return false; }
       isFile(): this is AsyncArrowFile { return false; }
     isStream(): this is AsyncArrowStream { return true; }
       isSync(): this is ArrowStream { return false; }
      isAsync(): this is AsyncArrowStream { return true; }
    async next(size?: number) {
        const r = (await super.next(size)) as IteratorResult<any>;
        !r.done && (r.value = new ByteBuffer(toUint8Array(r.value)));
        return r as IteratorResult<ByteBuffer>;
    }
}

class JSONResolver implements InputResolver {
     isSync(): this is InputResolver { return true; }
    isAsync(): this is AsyncInputResolver { return false; }
    constructor(private source: object) {}
    resolve() { return new ArrowJSON(this.source); }
}

class ByteStreamResolver implements InputResolver {
     isSync(): this is InputResolver { return true; }
    isAsync(): this is AsyncInputResolver { return false; }
    constructor(private source: ByteStream) {}
    resolve() {
        let { source } = this;
        let r = source.peek(magicLength);
        return !r.done
            ? checkForMagicArrowString(r.value)
            ? new ArrowFile(source.next())
            : new ArrowStream(source)
            : null;
    }
}

class AsyncByteStreamResolver implements AsyncInputResolver {
     isSync(): this is InputResolver { return false; }
    isAsync(): this is AsyncInputResolver { return true; }
    constructor(private source: AsyncByteStream) {}
    async resolve() {
        let { source } = this;
        let r = await source.peek(magicLength);
        return !r.done
            ? checkForMagicArrowString(r.value)
            ? new ArrowFile(await source.next())
            : new AsyncArrowStream(source)
            : null;
    }
}

class PromiseResolver implements AsyncInputResolver {
     isSync(): this is InputResolver { return false; }
    isAsync(): this is AsyncInputResolver { return true; }
    constructor(private source: PromiseLike<FileHandle> | PromiseLike<ArrayBufferView>) {}
    async resolve() {
        return await resolveInputFormat(await this.source).resolve();
    }
}

class FileHandleResolver implements AsyncInputResolver {
     isSync(): this is InputResolver { return false; }
    isAsync(): this is AsyncInputResolver { return true; }
    constructor(private source: FileHandle) {}
    async resolve() {
        let buffer: Uint8Array;
        let { source: file } = this;
        let { size } = await file.stat();
        if (size >= magicX2AndPadding) {
            buffer = new Uint8Array(magicLength);
            await file.read(buffer, 0, magicLength, 0);
            if (checkForMagicArrowString(buffer)) {
                return new AsyncArrowFile(file, size);
            }
        }
        (buffer = await file.readFile()) && await file.close();
        return new ArrowStream(fromIterable(buffer));
    }
}
