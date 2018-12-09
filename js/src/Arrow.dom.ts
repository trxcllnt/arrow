import { DataType } from './type';
import streamAdapters from './io/adapters';
import { RecordBatch } from './recordbatch';
import { RecordBatchReader } from './ipc/reader';
import { AsyncWritableByteStream } from './io/stream';
import { ReadableDOMStreamOptions } from './io/interfaces';
import { isIterable, isAsyncIterable } from './util/compat';

streamAdapters.toReadableDOMStream = toReadableDOMStream;
RecordBatchReader.asDOMStream = recordBatchReaderAsDOMStream;

export * from './Arrow';

function recordBatchReaderAsDOMStream<T extends { [key: string]: DataType } = any>() {

    let duplex = new AsyncWritableByteStream();
    let reader: RecordBatchReader<T> | null = null;

    const readable = new ReadableStream<RecordBatch<T>>({
        async start(controller) { await next(controller, reader || (reader = await open())); },
        async pull(controller) { reader ? await next(controller, reader) : controller.close(); },
        async cancel() { (reader && (await reader.close()) || true) && (reader = null); },
    });

    return { writable: new WritableStream<Uint8Array>(duplex), readable };

    async function open() {
        return await (await RecordBatchReader.from(duplex)).open();
    }

    async function next(controller: ReadableStreamDefaultController<RecordBatch<T>>, reader: RecordBatchReader<T>) {
        let size = controller.desiredSize;
        let r: IteratorResult<RecordBatch<T>> | null = null;
        while ((size == null || size-- > 0) && !(r = await reader.next()).done) {
            controller.enqueue(r.value);
        }
        r && r.done && controller.close();
    }
}

function toReadableDOMStream<T>(source: Iterable<T> | AsyncIterable<T>, options?: ReadableDOMStreamOptions): ReadableStream<T> {
    if (isAsyncIterable<T>(source)) { return asyncIterableAsReadableDOMStream(source, options); }
    if (isIterable<T>(source)) { return iterableAsReadableDOMStream(source, options); }
    throw new Error(`toReadableDOMStream() must be called with an Iterable or AsyncIterable`);
}

function iterableAsReadableDOMStream<T>(source: Iterable<T>, options?: ReadableDOMStreamOptions) {

    let it: Iterator<T> | null = null;

    return new ReadableStream<T>({
        ...options as any,
        start(controller) { next(controller, it || (it = source[Symbol.iterator]())); },
        pull(controller) { it ? (next(controller, it)) : controller.close(); },
        cancel() { (it && (it.return && it.return()) || true) && (it = null); }
    });

    function next(controller: ReadableStreamDefaultController<T>, it: Iterator<T>) {
        let size = controller.desiredSize;
        let r: IteratorResult<T> | null = null;
        while ((size == null || size-- > 0) && !(r = it.next()).done) {
            controller.enqueue(r.value);
        }
        r && r.done && controller.close();
    }
}

function asyncIterableAsReadableDOMStream<T>(source: AsyncIterable<T>, options?: ReadableDOMStreamOptions) {

    let it: AsyncIterator<T> | null = null;

    return new ReadableStream<T>({
        ...options as any,
        async start(controller) { await next(controller, it || (it = source[Symbol.asyncIterator]())); },
        async pull(controller) { it ? (await next(controller, it)) : controller.close(); },
        async cancel() { (it && (it.return && await it.return()) || true) && (it = null); },
    });

    async function next(controller: ReadableStreamDefaultController<T>, it: AsyncIterator<T>) {
        let size = controller.desiredSize;
        let r: IteratorResult<T> | null = null;
        while ((size == null || size-- > 0) && !(r = await it.next()).done) {
            controller.enqueue(r.value);
        }
        r && r.done && controller.close();
    }
}
