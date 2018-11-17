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

type ReadableNodeStream = NodeJS.ReadableStream;
type ReadStream = import('fs').ReadStream;
type FileHandle = import('fs').promises.FileHandle;
type ReadableDOMStream<R = any> = import('whatwg-streams').ReadableStream<R>;

export interface Subscription {
    unsubscribe: () => void;
}

export interface Observer<T> {
    closed?: boolean;
    next: (value: T) => void;
    error: (err: any) => void;
    complete: () => void;
}

export interface Observable<T> {
    subscribe: (observer: Observer<T>) => Subscription;
}

const isObject = (x: any) => x != null && Object(x) === x;
const hasFuncs = (x: any, ...fn: PropertyKey[]) => hasProps(x, ...fn.map((f) => [f, 'function'] as [PropertyKey, string]));
const hasProps = (x: any, ...ks: [PropertyKey, string?][]) => isObject(x) && ks.every(([k, t]) => t ? (typeof x[k] === t) : (k in x));

/** @ignore */ export const isPromise            = <T = any>(x: any): x is PromiseLike<T>       => hasFuncs(x, 'then');
/** @ignore */ export const isObservable         = <T = any>(x: any): x is Observable<T>        => hasFuncs(x, 'subscribe');
/** @ignore */ export const isIterable           = <T = any>(x: any): x is Iterable<T>          => hasFuncs(x, Symbol.iterator);
/** @ignore */ export const isAsyncIterable      = <T = any>(x: any): x is AsyncIterable<T>     => hasFuncs(x, Symbol.asyncIterator);
/** @ignore */ export const isArrowJSON          =          (x: any): x is object               => hasProps(x, ['schema', 'object']);
/** @ignore */ export const isArrayLike          = <T = any>(x: any): x is ArrayLike<T>         => hasProps(x, ['length', 'number']);
/** @ignore */ export const isIteratorResult     = <T = any>(x: any): x is IteratorResult<T>    => hasProps(x, ['done'], ['value']);
/** @ignore */ export const isReadableDOMStream  = <T = any>(x: any): x is ReadableDOMStream<T> => hasFuncs(x, 'getReader', 'pipeTo', 'cancel', 'tee');
/** @ignore */ export const isReadableNodeStream =          (x: any): x is ReadableNodeStream   => hasFuncs(x, 'read', 'pipe', 'unpipe', 'pause', 'resume', 'wrap');
/** @ignore */ export const isFileHandle         =          (x: any): x is FileHandle           => hasFuncs(x, 'stat') && hasProps(x, ['fd', 'number']);
/** @ignore */ export const isFileReadStream     =          (x: any): x is ReadStream           => isReadableNodeStream(x) && hasProps(x, ['bytesRead', 'number']);
