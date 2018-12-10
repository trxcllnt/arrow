import { DataType } from './type';
import { Duplex, Readable } from 'stream';
import streamAdapters from './io/adapters';
import { RecordBatch } from './recordbatch';
import { RecordBatchReader } from './ipc/reader';
import { AsyncWritableByteStream } from './io/stream';
import { isIterable, isAsyncIterable } from './util/compat';

type ReadableOptions = import('stream').ReadableOptions;

streamAdapters.toReadableNodeStream = toReadableNodeStream;
RecordBatchReader.asNodeStream = recordBatchReaderAsNodeStream;

export * from './Arrow.dom';

function recordBatchReaderAsNodeStream<T extends { [key: string]: DataType } = any>() {

    let blocked = false;
    let duplex = new AsyncWritableByteStream();
    let reader: RecordBatchReader<T> | null = null;

    return new Duplex({
        allowHalfOpen: false,
        readableObjectMode: true,
        writableObjectMode: false,
        write(...args: any[]) { duplex.write(...args); },
        final(...args: any[]) { duplex.final(...args); },
        read(size: number): void {
            blocked || (blocked = !!(async () => (
                await next(this, size, reader || (reader = await open()))
            ))());
        },
        destroy(...args: any[]) {
            blocked = true;
            (async () => await (reader && reader.close()))()
                .catch((error) => (args[0] = error))
                .then(() => duplex.destroy(...args))
                .then(() => reader = null);
        }
    });

    async function open() {
        return await (await RecordBatchReader.from(duplex)).open();
    }

    async function next(sink: Readable, size: number, reader: RecordBatchReader<T>): Promise<any> {
        let r: IteratorResult<RecordBatch<T>> | null = null;
        while (sink.readable && (size == null || size-- > 0) && !(r = await reader.next()).done) {
            if (!sink.push(r.value)) { return blocked = false; }
        }
        if (((r && r.done) || !sink.readable) && (blocked = sink.push(null) || true)) {
            reader.return && await reader.return();
        }
    }
}

/**
 * @ignore
 */
function toReadableNodeStream<T>(source: Iterable<T> | AsyncIterable<T>, options?: ReadableOptions): Readable {
    if (isAsyncIterable<T>(source)) { return asyncIterableAsReadableNodeStream(source, options); }
    if (isIterable<T>(source)) { return iterableAsReadableNodeStream(source, options); }
    throw new Error(`toReadableNodeStream() must be called with an Iterable or AsyncIterable`);
}

function iterableAsReadableNodeStream<T>(source: Iterable<T>, options?: ReadableOptions) {
    let it: Iterator<T>, blocked = false;
    return new Readable({
        ...options,
        read(size: number) {
            !blocked && (blocked = true) &&
                next(this, size, (it || (it = source[Symbol.iterator]())));
        },
        destroy(e: Error | null, cb: (e: Error | null) => void) {
            if ((blocked = true) && it || Boolean(cb(null))) {
                let fn = e == null ? it.return : it.throw;
                (fn && fn.call(it, e) || true) && cb(null);
            }
        },
    });
    function next(sink: Readable, size: number, it: Iterator<T>): any {
        let r: IteratorResult<T> | null = null;
        while (sink.readable && (size == null || size-- > 0) && !(r = it.next()).done) {
            if (!sink.push(r.value)) { return blocked = false; }
        }
        if (((r && r.done) || !sink.readable) && (blocked = sink.push(null) || true)) {
            it.return && it.return();
        }
    }
}

function asyncIterableAsReadableNodeStream<T>(source: AsyncIterable<T>, options?: ReadableOptions) {
    let it: AsyncIterator<T>, blocked = false;
    return new Readable({
        ...options,
        read(size: number) {
            blocked || (blocked = !!(async () => (
                await next(this, size, (it || (it = source[Symbol.asyncIterator]())))
            ))());
        },
        destroy(e: Error | null, cb: (e: Error | null) => void) {
            if ((blocked = true) && it || Boolean(cb(null))) {
                (async (fn) => {
                    (fn && await fn.call(it, e) || true) && cb(null)
                })(e == null ? it.return : it.throw);
            }
        },
    });
    async function next(sink: Readable, size: number, it: AsyncIterator<T>): Promise<any> {
        let r: IteratorResult<T> | null = null;
        while (sink.readable && (size == null || size-- > 0) && !(r = await it.next()).done) {
            if (!sink.push(r.value)) { return blocked = false; }
        }
        if (((r && r.done) || !sink.readable) && (blocked = sink.push(null) || true)) {
            it.return && await it.return();
        }
    }
}
