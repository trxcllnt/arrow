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

import { ReadableInterop, ArrowJSONLike } from '../io/interfaces';

/** @ignore */
type FSReadStream = import('fs').ReadStream;
/** @ignore */
type FileHandle = import('fs').promises.FileHandle;

/** @ignore */
export interface Subscription {
    unsubscribe: () => void;
}

/** @ignore */
export interface Observer<T> {
    closed?: boolean;
    next: (value: T) => void;
    error: (err: any) => void;
    complete: () => void;
}

/** @ignore */
export interface Observable<T> {
    subscribe: (observer: Observer<T>) => Subscription;
}

/** @ignore */ const isObject = (x: any) => x != null && Object(x) === x;
/** @ignore */ const hasFuncs = (x: any, ...fn: PropertyKey[]) => hasProps(x, ...fn.map((f) => [f, 'function'] as [PropertyKey, string]));
/** @ignore */ const hasProps = (x: any, ...ks: [PropertyKey, string?][]) => isObject(x) && ks.every(([k, t]) => t ? (x[k] != null && typeof x[k] === t) : (k in x));

/** @ignore */ export const isPromise            = <T = any>(x: any): x is PromiseLike<T>        => hasFuncs(x, 'then');
/** @ignore */ export const isObservable         = <T = any>(x: any): x is Observable<T>         => hasFuncs(x, 'subscribe');
/** @ignore */ export const isIterable           = <T = any>(x: any): x is Iterable<T>           => hasFuncs(x, Symbol.iterator);
/** @ignore */ export const isAsyncIterable      = <T = any>(x: any): x is AsyncIterable<T>      => hasFuncs(x, Symbol.asyncIterator);
/** @ignore */ export const isArrowJSON          =          (x: any): x is ArrowJSONLike         => hasProps(x, ['schema', 'object']);
/** @ignore */ export const isArrayLike          = <T = any>(x: any): x is ArrayLike<T>          => hasProps(x, ['length', 'number']);
/** @ignore */ export const isIteratorResult     = <T = any>(x: any): x is IteratorResult<T>     => hasProps(x, ['done'], ['value']);
/** @ignore */ export const isUnderlyingSink     = <T = any>(x: any): x is UnderlyingSink<T>     => hasFuncs(x, 'abort', 'close', 'start', 'write');
/** @ignore */ export const isFileHandle         =          (x: any): x is FileHandle            => hasFuncs(x, 'stat') && hasProps(x, ['fd', 'number']);
/** @ignore */ export const isFSReadStream       =          (x: any): x is FSReadStream          => isReadableNodeStream(x) && hasProps(x, ['bytesRead', 'number']);
/** @ignore */ export const isFetchResponse      =          (x: any): x is Response              => hasProps(x, ['body'], ['bodyUsed', 'boolean'], ['ok', 'boolean']);
/** @ignore */ export const isWritableDOMStream  = <T = any>(x: any): x is WritableStream<T>     => !(x instanceof ReadableInterop) && hasFuncs(x, 'abort', 'getWriter');
/** @ignore */ export const isReadableDOMStream  = <T = any>(x: any): x is ReadableStream<T>     => !(x instanceof ReadableInterop) && hasFuncs(x, 'tee', 'cancel', 'pipeTo', 'getReader');
/** @ignore */ export const isWritableNodeStream =          (x: any): x is NodeJS.WritableStream => !(x instanceof ReadableInterop) && hasFuncs(x, 'write', 'end') && hasProps(x, ['writable', 'boolean']);
/** @ignore */ export const isReadableNodeStream =          (x: any): x is NodeJS.ReadableStream => !(x instanceof ReadableInterop) && hasFuncs(x, 'read', 'pipe') && hasProps(x, ['readable', 'boolean']);
