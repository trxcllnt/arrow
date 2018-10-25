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

import { ArrayBufferViewConstructor  } from '../interfaces';
import { isIteratorResult, isIterable, isAsyncIterable } from './compat';

/**
 * @ignore
 */
export function concat(chunks: Uint8Array[], size?: number | null): [Uint8Array, Uint8Array[]] {
    let offset = 0, index = -1, chunksLen = chunks.length;
    let source: Uint8Array, sliced: Uint8Array, buffer: Uint8Array | void;
    let length = typeof size === 'number' ? size : chunks.reduce((x, y) => x + y.length, 0);
    while (++index < chunksLen) {
        source = chunks[index];
        sliced = source.subarray(0, Math.min(source.length, length - offset));
        if (length <= (offset += sliced.length)) {
            if (sliced.length < source.length) {
                chunks[index] = source.subarray(sliced.length);
            } else if (sliced.length === source.length) { index++; }
            buffer ? buffer.set(sliced, offset) : (buffer = sliced);
            break;
        }
        (buffer || (buffer = new Uint8Array(length))).set(sliced, offset);
    }
    return [buffer || new Uint8Array(0), chunks.slice(index)];
}

type ArrayBufferViewInput = ArrayBufferLike | ArrayBufferView | string | null | undefined |
             IteratorResult<ArrayBufferLike | ArrayBufferView | string | null | undefined>;

/**
 * @ignore
 */
export function toArrayBufferView<T extends ArrayBufferView>(ArrayBufferViewCtor: ArrayBufferViewConstructor<T>, input: ArrayBufferViewInput): T {

    let value: any = isIteratorResult(input) ? input.value : input;

    if (!value) { return new ArrayBufferViewCtor(0); }
    if (typeof value === 'string') { value = decodeUtf8(value); }
    if (value instanceof ArrayBuffer) { return new ArrayBufferViewCtor(value); }
    if (value instanceof SharedArrayBuffer) { return new ArrayBufferViewCtor(value); }
    return !ArrayBuffer.isView(value) ? ArrayBufferViewCtor.from(value) :
        new ArrayBufferViewCtor(value.buffer, value.byteOffset, value.byteLength);
}

/** @ignore */ export const toInt8Array = (input: ArrayBufferViewInput) => toArrayBufferView(Int8Array, input);
/** @ignore */ export const toInt16Array = (input: ArrayBufferViewInput) => toArrayBufferView(Int16Array, input);
/** @ignore */ export const toInt32Array = (input: ArrayBufferViewInput) => toArrayBufferView(Int32Array, input);
/** @ignore */ export const toUint8Array = (input: ArrayBufferViewInput) => toArrayBufferView(Uint8Array, input);
/** @ignore */ export const toUint16Array = (input: ArrayBufferViewInput) => toArrayBufferView(Uint16Array, input);
/** @ignore */ export const toUint32Array = (input: ArrayBufferViewInput) => toArrayBufferView(Uint32Array, input);
/** @ignore */ export const toFloat32Array = (input: ArrayBufferViewInput) => toArrayBufferView(Float32Array, input);
/** @ignore */ export const toFloat64Array = (input: ArrayBufferViewInput) => toArrayBufferView(Float64Array, input);
/** @ignore */ export const toUint8ClampedArray = (input: ArrayBufferViewInput) => toArrayBufferView(Uint8ClampedArray, input);

type ArrayBufferViewIteratorInput = Iterable<ArrayBufferViewInput> | ArrayBufferViewInput;

/** @ignore */
export function* toArrayBufferViewIterator<T extends ArrayBufferView>(ArrayCtor: ArrayBufferViewConstructor<T>, source: ArrayBufferViewIteratorInput) {

    const wrap = function*<T>(x: T) { yield x; };
    const buffers: Iterable<ArrayBufferViewInput> =
                   (typeof source === 'string') ? wrap(source)
                 : (ArrayBuffer.isView(source)) ? wrap(source)
              : (source instanceof ArrayBuffer) ? wrap(source)
        : (source instanceof SharedArrayBuffer) ? wrap(source)
    : !isIterable<ArrayBufferViewInput>(source) ? wrap(source) : source;

    for (let x of buffers) {
        if (x) {
            yield toArrayBufferView(ArrayCtor, x);
        }
    }
}

/** @ignore */ export const toInt8ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Int8Array, input);
/** @ignore */ export const toInt16ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Int16Array, input);
/** @ignore */ export const toInt32ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Int32Array, input);
/** @ignore */ export const toUint8ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Uint8Array, input);
/** @ignore */ export const toUint16ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Uint16Array, input);
/** @ignore */ export const toUint32ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Uint32Array, input);
/** @ignore */ export const toFloat32ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Float32Array, input);
/** @ignore */ export const toFloat64ArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Float64Array, input);
/** @ignore */ export const toUint8ClampedArrayIterator = (input: ArrayBufferViewIteratorInput) => toArrayBufferViewIterator(Uint8ClampedArray, input);

type ArrayBufferViewAsyncIteratorInput = AsyncIterable<ArrayBufferViewInput> | Iterable<ArrayBufferViewInput> | PromiseLike<ArrayBufferViewInput> | ArrayBufferViewInput;

/** @ignore */
export async function* toArrayBufferViewAsyncIterator<T extends ArrayBufferView>(ArrayCtor: ArrayBufferViewConstructor<T>, source: ArrayBufferViewAsyncIteratorInput) {

    const wrap = async function*<T>(x: T) { yield await x; };
    const emit = async function*<T>(x: Iterable<T>) { yield* x; };
    const buffers: AsyncIterable<ArrayBufferViewInput> =
                      (typeof source === 'string') ? wrap(source)
                    : (ArrayBuffer.isView(source)) ? wrap(source)
                 : (source instanceof ArrayBuffer) ? wrap(source)
           : (source instanceof SharedArrayBuffer) ? wrap(source)
        : isIterable<ArrayBufferViewInput>(source) ? emit(source)
  : !isAsyncIterable<ArrayBufferViewInput>(source) ? wrap(source as ArrayBufferViewInput) : source;

    for await (let x of buffers) {
        if (x) {
            yield toArrayBufferView(ArrayCtor, x);
        }
    }
}

/** @ignore */ export const toInt8ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Int8Array, input);
/** @ignore */ export const toInt16ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Int16Array, input);
/** @ignore */ export const toInt32ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Int32Array, input);
/** @ignore */ export const toUint8ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Uint8Array, input);
/** @ignore */ export const toUint16ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Uint16Array, input);
/** @ignore */ export const toUint32ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Uint32Array, input);
/** @ignore */ export const toFloat32ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Float32Array, input);
/** @ignore */ export const toFloat64ArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Float64Array, input);
/** @ignore */ export const toUint8ClampedArrayAsyncIterator = (input: ArrayBufferViewAsyncIteratorInput) => toArrayBufferViewAsyncIterator(Uint8ClampedArray, input);

/**
 * @ignore
 */
function decodeUtf8(chunk: string) {
    const bytes = new Uint8Array(chunk.length);
    for (let i = -1, n = chunk.length; ++i < n;) {
        bytes[i] = chunk.charCodeAt(i);
    }
    return bytes;
}
