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

import { toUint8Array } from '../util/buffer';
import { IteratorBase, AsyncIteratorBase } from './interfaces';
import { ReadableDOMStream, WritableDOMStream, ReadableNodeStream } from './interfaces';

type PipeOptions = import('whatwg-streams').PipeOptions;
type WritableReadablePair<
    T extends WritableDOMStream<any>,
    U extends ReadableDOMStream<any>
> = import('whatwg-streams').WritableReadablePair<T, U>;

/**
 * @ignore
 */
export class ByteStream<TResult = Uint8Array, TSource = Uint8Array> extends IteratorBase<TResult, Iterator<TSource>> {
    peek(size?: number) { return super.next(typeof size === 'object' && size || { cmd: 'peek', size }); }
    next(size?: number) { return super.next(typeof size === 'object' && size || { cmd: 'read', size }); }
    pipe<T extends NodeJS.WritableStream>(writable: T, options?: { end?: boolean; }) {
        return this.asReadableNodeStream().pipe(writable, options);
    }
    pipeTo(writable: WritableDOMStream<Uint8Array>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeTo(writable, options);
    }
    pipeThrough<T extends ReadableDOMStream<any>>(duplex: WritableReadablePair<WritableDOMStream<Uint8Array>, T>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeThrough(duplex, options);
    }
    asReadableDOMStream() {
        let self = this, it: Iterator<any>;
        return new ReadableDOMStream<Uint8Array>({
            cancel: close.bind(null, 'return'),
            start() { it = self[Symbol.iterator](); },
            pull(controller) {
                try {
                    let size = controller.desiredSize, r: IteratorResult<any>;
                    while ((size == null || size > 0) && !(r = it.next(size)).done) {
                        const value = toUint8Array(r.value);
                        if (size != null) { size -= value.byteLength; }
                        if (value.length > 0) { controller.enqueue(value); }
                    }
                    close('return');
                    controller.close();
                } catch (e) {
                    close('throw', e);
                    controller.error(e);
                }
            }
        });
        function close(signal: 'throw' | 'return', value?: any) {
            if (it && typeof it[signal] === 'function') {
                it[signal]!(value);
            }
        }
    }
    asReadableNodeStream() {
        let self = this, it: Iterator<TResult>;
        return new ReadableNodeStream({
            read(size?: number) {
                (it || (it = self[Symbol.iterator]())) && read(this, size);
            },
            destroy(e: Error | null, cb: (e: Error | null) => void) {
                const signal = e == null ? 'return' : 'throw';
                try { close(signal, e); } catch (err) {
                    return cb && Promise.resolve(err).then(cb);
                }
                return cb && Promise.resolve(null).then(cb);
            }
        });
        function read(sink: ReadableNodeStream, size?: number) {
            let r: IteratorResult<any> | null = null;
            while ((size == null || size > 0) && !(r = it.next(size)).done) {
                const value = toUint8Array(r.value);
                if (size) { size -= value.length; }
                if (value.length <= 0) { continue; }
                if (!sink.push(value)) { return; }
            }
            r && r.done && end(sink);
        }
        function close(signal: 'throw' | 'return', value?: any) {
            if (it && typeof it[signal] === 'function') {
                it[signal]!(value);
            }
        }
        function end(sink: ReadableNodeStream) {
            sink.push(null);
            close('return');
        }
    }
}

/**
 * @ignore
 */
export class AsyncByteStream<TResult = Uint8Array, TSource = Uint8Array> extends AsyncIteratorBase<TResult, AsyncIterator<TSource>> {
    async peek(size?: number) { return await super.next(typeof size === 'object' && size || { cmd: 'peek', size }); }
    async next(size?: number) { return await super.next(typeof size === 'object' && size || { cmd: 'read', size }); }
    pipe<T extends NodeJS.WritableStream>(writable: T, options?: { end?: boolean; }) {
        return this.asReadableNodeStream().pipe(writable, options);
    }
    pipeTo(writable: WritableDOMStream<Uint8Array>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeTo(writable, options);
    }
    pipeThrough<T extends ReadableDOMStream<any>>(duplex: WritableReadablePair<WritableDOMStream<Uint8Array>, T>, options?: PipeOptions) {
        return this.asReadableDOMStream().pipeThrough(duplex, options);
    }
    asReadableDOMStream() {
        let self = this, it: AsyncIterator<any>;
        return new ReadableDOMStream<Uint8Array>({
            cancel: close.bind(null, 'return'),
            async start() { it = self[Symbol.asyncIterator](); },
            async pull(controller) {
                try {
                    let size = controller.desiredSize, r: IteratorResult<any>;
                    while ((size == null || size > 0) && !(r = await it.next(size)).done) {
                        const value = toUint8Array(r.value);
                        if (size != null) { size -= value.byteLength; }
                        if (value.length > 0) { controller.enqueue(value); }
                    }
                    await Promise.all([close('return'), controller.close()]);
                } catch (e) {
                    await Promise.all([close('throw', e), controller.error(e)]);
                }
            }
        });
        async function close(signal: 'throw' | 'return', value?: any) {
            if (it && typeof it[signal] === 'function') {
                await it[signal]!(value);
            }
        }
    }
    asReadableNodeStream() {
        let self = this, it: AsyncIterator<TResult>;
        return new ReadableNodeStream({
            read(size?: number) {
                (it || (it = self[Symbol.asyncIterator]())) && read(this, size);
            },
            destroy(e: Error | null, cb: (e: Error | null) => void) {
                close(e == null ? 'return' : 'throw', e).then(cb as any, cb);
            }
        });
        async function read(sink: ReadableNodeStream, size?: number) {
            let r: IteratorResult<any> | null = null;
            while ((size == null || size > 0) && !(r = await it.next(size)).done) {
                const value = toUint8Array(r.value);
                if (value.length <= 0) { continue; }
                if (size) { size -= value.length; }
                if (!sink.push(value)) { return; }
            }
            r && r.done && await end(sink);
        }
        async function close(signal: 'throw' | 'return', value?: any) {
            if (it && typeof it[signal] === 'function') {
                await it[signal]!(value);
            }
        }
        async function end(sink: ReadableNodeStream) {
            sink.push(null);
            await close('return');
        }
    }
}
