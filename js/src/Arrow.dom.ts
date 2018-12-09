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
        async start(observer) { await next(observer, reader || await open()); },
        async pull(observer) { reader ? await next(observer, reader) : observer.close(); },
        async cancel() { (reader && (await reader.close())) && false || (reader = null); },
    });

    return { writable: new WritableStream<Uint8Array>(duplex), readable };

    async function open() {
        return await (await RecordBatchReader.from(duplex)).open();
    }

    async function next(sink: ReadableStreamDefaultController<RecordBatch<T>>, reader: RecordBatchReader<T>) {
        let size = sink.desiredSize;
        let r: IteratorResult<RecordBatch<T>> | null = null;
        while ((size == null || size-- > 0) && !(r = await reader.next()).done) {
            sink.enqueue(r.value);
        }
        r && r.done && sink.close();
    }
}

function toReadableDOMStream<T>(source: Iterable<T> | AsyncIterable<T>, options?: ReadableDOMStreamOptions): ReadableStream<T> {
    if (isAsyncIterable<T>(source)) { return asyncIterableAsReadableDOMStream(source, options); }
    if (isIterable<T>(source)) { return iterableAsReadableDOMStream(source, options); }
    throw new Error(`toReadableDOMStream() must be called with an Iterable or AsyncIterable`);
}

function iterableAsReadableDOMStream<T>(source: Iterable<T>, options?: ReadableDOMStreamOptions) {
    let it: Iterator<T>;
    return new ReadableStream<T>({
        ...options as any,
        cancel: close.bind(null, 'return'),
        start() { it = source[Symbol.iterator](); },
        pull(sink) {
            try {
                let size = sink.desiredSize;
                let r: IteratorResult<T> | null = null;
                while ((size == null || size-- > 0) && !(r = it.next()).done) {
                    sink.enqueue(r.value);
                }
                r && r.done && [close('return'), sink.close()];
            } catch (e) {
                close('throw', e);
                sink.error(e);
            }
        }
    });
    function close(signal: 'throw' | 'return', value?: any) {
        if (it && typeof it[signal] === 'function') {
            it[signal]!(value);
        }
    }
}

function asyncIterableAsReadableDOMStream<T>(source: AsyncIterable<T>, options?: ReadableDOMStreamOptions) {
    let it: AsyncIterator<T>;
    return new ReadableStream<T>({
        ...options as any,
        cancel: close.bind(null, 'return'),
        async start() { it = source[Symbol.asyncIterator](); },
        async pull(controller) {
            try {
                let size = controller.desiredSize;
                let r: IteratorResult<T> | null = null;
                while ((size == null || size-- > 0) && !(r = await it.next()).done) {
                    controller.enqueue(r.value);
                }
                r && r.done && (await Promise.all([close('return'), controller.close()]));
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
