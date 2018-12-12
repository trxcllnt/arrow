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

import streamAdapters from './adapters';
import { memcpy, toUint8Array, ArrayBufferViewInput } from '../util/buffer';
import {
    ITERATOR_DONE,
    Readable, Writable, ReadableWritable,
    ReadableInterop, AsyncQueue, ReadableDOMStreamOptions
} from './interfaces';
import {
    isPromise, isFetchResponse,
    isIterable, isAsyncIterable,
    isReadableDOMStream, isReadableNodeStream,
    isWritableDOMStream, isWritableNodeStream
} from '../util/compat';

export type WritableSink<T> = Writable<T> | WritableStream<T> | NodeJS.WritableStream | null;
export type ReadableSource<T> = Readable<T> | PromiseLike<T> | AsyncIterable<T> | ReadableStream<T> | NodeJS.ReadableStream | null;

/**
 * @ignore
 */
export class AsyncByteQueue<T extends ArrayBufferViewInput = Uint8Array> extends AsyncQueue<Uint8Array, T> {
    public write(value: ArrayBufferViewInput | Uint8Array) {
        if ((value = toUint8Array(value)).byteLength > 0) {
            return super.write(value as T);
        }
    }
    public async toUint8Array() {
        let chunks = [], total = 0;
        for await (const chunk of this) {
            chunks.push(chunk);
            total += chunk.byteLength;
        }
        return chunks.reduce((x, buffer) => {
            x.buffer.set(buffer, x.offset);
            x.offset += buffer.byteLength;
            return x;
        }, { offset: 0, buffer: new Uint8Array(total) }).buffer;
    }
}

/**
 * @ignore
 */
export class ByteStream {
    // @ts-ignore
    private source: ByteStreamSource<Uint8Array | null>;
    constructor(source?: Iterable<ArrayBufferViewInput> | ArrayBufferViewInput) {
        if (source) {
            this.source = new ByteStreamSource(streamAdapters.fromIterable(source));
        }
    }
    public throw(value?: any) { return this.source.throw(value); }
    public return(value?: any) { return this.source.return(value); }
    public peek(size?: number | null) { return this.source.peek(size); }
    public read(size?: number | null) { return this.source.read(size); }
}

/**
 * @ignore
 */
export class AsyncByteStream implements Readable<Uint8Array> {
    // @ts-ignore
    private source: AsyncByteStreamSource<Uint8Array>;
    constructor(source?: PromiseLike<ArrayBufferViewInput> | Response | ReadableStream<ArrayBufferViewInput> | NodeJS.ReadableStream | AsyncIterable<ArrayBufferViewInput> | Iterable<ArrayBufferViewInput>) {
        if (!source) {}
        else if (isReadableNodeStream(source)) { this.source = new AsyncByteStreamSource(streamAdapters.fromReadableNodeStream(source)); }
        else if (isFetchResponse(source)) { this.source = new AsyncByteStreamSource(streamAdapters.fromReadableDOMStream(source.body!)); }
        else if (isIterable<ArrayBufferViewInput>(source)) { this.source = new AsyncByteStreamSource(streamAdapters.fromIterable(source)); }
        else if (isPromise<ArrayBufferViewInput>(source)) { this.source = new AsyncByteStreamSource(streamAdapters.fromAsyncIterable(source)); }
        else if (isAsyncIterable<ArrayBufferViewInput>(source)) { this.source = new AsyncByteStreamSource(streamAdapters.fromAsyncIterable(source)); }
        else if (isReadableDOMStream<ArrayBufferViewInput>(source)) { this.source = new AsyncByteStreamSource(streamAdapters.fromReadableDOMStream(source)); }
    }
    public next(value?: any) { return this.source.next(value); }
    public throw(value?: any) { return this.source.throw(value); }
    public return(value?: any) { return this.source.return(value); }
    public get closed(): Promise<void> { return this.source.closed; }
    public cancel(reason?: any) { return this.source.cancel(reason); }
    public peek(size?: number | null) { return this.source.peek(size); }
    public read(size?: number | null) { return this.source.read(size); }
}

/**
 * @ignore
 */
export class AsyncArrowStream<TReadable = Uint8Array, TWritable = TReadable> extends ReadableInterop<TReadable>
    implements ReadableWritable<TReadable, TWritable>,
               ReadableStream<TReadable>,
               WritableStream<TWritable> {

    // @ts-ignore
    private reader: Readable<TReadable>;
    // @ts-ignore
    private writer: Writable<TWritable>;

    constructor(source?: ReadableSource<TReadable>, sink?: WritableSink<TWritable>) {
        super();

        let writer: AsyncQueue<TWritable> | AsyncArrowStream;

        if ((sink instanceof AsyncQueue) || (sink instanceof AsyncArrowStream)) {
            writer = sink;
        } else if (writer = new AsyncQueue<TWritable>()) {
            if (isWritableDOMStream(sink)) { writer.toReadableDOMStream().pipeTo(sink); }
            else if (isWritableNodeStream(sink)) { writer.toReadableNodeStream().pipe(sink); }
        }

        this.writer = writer as Writable<TWritable>;
        if ((source instanceof AsyncByteStream) || (source instanceof AsyncArrowStream)) {
            this.reader = <any> source;
        } else {
            this.reader = <any> new AsyncByteStream(writer[Symbol.asyncIterator]() as any);
        }
    }

    public get locked(): boolean { return false; }
    public get closed() {
        return Promise.all([this.writer.closed, this.reader.closed]).then(() => undefined);
    }
    public [Symbol.asyncIterator]() { return this; }
    public getReader(): ReadableStreamDefaultReader<TReadable>;
    public getReader(options: { mode: "byob" }): ReadableStreamBYOBReader;
    public getReader(..._opt: { mode: "byob" }[]) { return new AsyncStreamReader(this.reader) as any; }
    public getWriter(): WritableStreamDefaultWriter<TWritable> { return new AsyncStreamWriter(this.writer); }

    public toReadableDOMStream(options?: ReadableDOMStreamOptions): ReadableStream<TReadable> {
        return streamAdapters.toReadableDOMStream(this, options);
    }
    public toReadableNodeStream(options?: import('stream').ReadableOptions): import('stream').Readable {
        return streamAdapters.toReadableNodeStream(this, options);
    }

    public close() { this.writer.close(); }
    public abort(reason?: any): Promise<void> {
        return Promise.resolve(this.writer.abort(reason));
    }
    public write(value: TWritable) { return this.writer.write(value); }

    public async cancel(reason?: any) { await this.reader.cancel(reason); }
    public async next(value?: any) { return await this.reader.next(value); }
    public async throw(value?: any) { return await this.reader.throw(value); }
    public async return(value?: any) { return await this.reader.return(value); }

    public async peek(size?: number | null) { return await this.reader.peek(size); }
    public async read(size?: number | null) { return await this.reader.read(size); }
}

class AsyncStreamReader<T> implements ReadableStreamDefaultReader<T>, ReadableStreamBYOBReader {
    constructor(private reader: Readable<T>) {}
    public releaseLock(): void {}
    public get closed(): Promise<void> { return this.reader.closed; }
    public async cancel(reason?: any): Promise<void> { await this.reader.return(reason); }
    public async read(): Promise<ReadableStreamReadResult<T>>;
    public async read<R extends ArrayBufferView>(view: R): Promise<ReadableStreamReadResult<T>>;
    public async read<R extends ArrayBufferView>(arg?: R): Promise<ReadableStreamReadResult<T | R>> {
        let result: IteratorResult<T | R> = await this.reader.next(arg ? arg.byteLength : arg);
        if (ArrayBuffer.isView(arg) && ArrayBuffer.isView(result.value)) {
            result.value = memcpy(arg, result.value);
        }
        return result;
    }
}

class AsyncStreamWriter<T> implements WritableStreamDefaultWriter<T> {
    constructor(private writer: Writable<T>) {}
    public releaseLock(): void {}
    public get desiredSize(): number | null { return Infinity; }
    public get ready(): Promise<void> { return Promise.resolve(); }
    public get closed(): Promise<void> { return this.writer.closed; }
    public close(): Promise<void> { return Promise.resolve(this.writer.close()); }
    public write(chunk: T): Promise<void> { return Promise.resolve(this.writer.write(chunk)); }
    public abort(reason?: any): Promise<void> { return Promise.resolve(this.writer.abort(reason)); }
}

interface ByteStreamSourceIterator<T> extends IterableIterator<T> {
    next(value?: { cmd: 'peek' | 'read', size?: number | null }): IteratorResult<T>;
}

interface AsyncByteStreamSourceIterator<T> extends AsyncIterableIterator<T> {
    next(value?: { cmd: 'peek' | 'read', size?: number | null }): Promise<IteratorResult<T>>;
}

class ByteStreamSource<T> {
    constructor(protected source: ByteStreamSourceIterator<T>) {}
    public cancel(reason?: any) { this.return(reason); }
    public peek(size?: number | null): T | null { return this.next(size, 'peek').value; }
    public read(size?: number | null): T | null { return this.next(size, 'read').value; }
    public next(size?: number | null, cmd: 'peek' | 'read' = 'read') { return this.source.next({ cmd, size }); }
    public throw(value?: any) { return Object.create((this.source.throw && this.source.throw(value)) || ITERATOR_DONE); }
    public return(value?: any) { return Object.create((this.source.return && this.source.return(value)) || ITERATOR_DONE); }
}

class AsyncByteStreamSource<T> implements Readable<T> {

    private _closedPromise: Promise<void>;
    private _closedPromiseResolve?: (value?: any) => void;
    constructor (protected source: ByteStreamSourceIterator<T> | AsyncByteStreamSourceIterator<T>) {
        this._closedPromise = new Promise((r) => this._closedPromiseResolve = r);
    }
    public async cancel(reason?: any) { await this.return(reason); }
    public get closed(): Promise<void> { return this._closedPromise; }
    public async read(size?: number | null): Promise<T | null> { return (await this.next(size, 'read')).value; }
    public async peek(size?: number | null): Promise<T | null> { return (await this.next(size, 'peek')).value; }
    public async next(size?: number | null, cmd: 'peek' | 'read' = 'read') { return (await this.source.next({ cmd, size })); }
    public async throw(value?: any) {
        const result = (this.source.throw && await this.source.throw(value)) || ITERATOR_DONE;
        this._closedPromiseResolve && this._closedPromiseResolve();
        this._closedPromiseResolve = undefined;
        return Object.create(result);
    }
    public async return(value?: any) {
        const result = (this.source.return && await this.source.return(value)) || ITERATOR_DONE;
        this._closedPromiseResolve && this._closedPromiseResolve();
        this._closedPromiseResolve = undefined;
        return Object.create(result);
    }
}
