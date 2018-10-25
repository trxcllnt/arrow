// import { Vector } from '../interfaces';
// import { flatbuffers } from 'flatbuffers';
// import { Schema, Long } from '../schema';
// import { RecordBatch } from '../recordbatch';
// import { isAsyncIterable } from '../util/compat';
// import { Message, DictionaryBatch, RecordBatchMetadata } from '../ipc/metadata';

import { flatbuffers } from 'flatbuffers';
import ByteBuffer = flatbuffers.ByteBuffer;
import { RecordBatch } from '../recordbatch';
import { Message, MessageReader } from './message';
import { Schema } from '../schema';
import { Vector } from '../interfaces';
import { MessageHeader } from '../enum';
import { schemaFromMessage } from './metadata-internal';

export const ITERATOR_DONE: any = Object.freeze({ done: true, value: void (0) });

export class IteratorBase<TResult, TSource extends Iterator<any> = Iterator<any>> implements Required<IterableIterator<TResult | null | void>> {
    constructor(protected source: TSource) {}
    [Symbol.iterator]() { return this; }
    next(value?: any) { return this.source && (this.source.next(value) as any) || ITERATOR_DONE; }
    throw(value?: any) { return this.source && this.source.throw && (this.source.throw(value) as any) || ITERATOR_DONE; }
    return(value?: any) { return this.source && this.source.return && (this.source.return(value) as any) || ITERATOR_DONE; }
}

export class AsyncIteratorBase<TResult, TSource extends AsyncIterator<any> = AsyncIterator<any>> implements Required<AsyncIterableIterator<TResult | null | void>> {
    constructor(protected source: TSource) {}
    [Symbol.asyncIterator]() { return this; }
    async next(value?: any) { return this.source && (await this.source.next(value) as any) || ITERATOR_DONE; }
    async throw(value?: any) { return this.source && this.source.throw && (await this.source.throw(value) as any) || ITERATOR_DONE; }
    async return(value?: any) { return this.source && this.source.return && (await this.source.return(value) as any) || ITERATOR_DONE; }
}

export class BufferReader extends IteratorBase<ByteBuffer, Iterator<Uint8Array>> {
    next(size?: number): IteratorResult<ByteBuffer> {
        const r = this.source.next(size) as IteratorResult<any>;
        !r.done && (r.value = new ByteBuffer(r.value));
        return r as IteratorResult<ByteBuffer>;
    }
}

export class AsyncBufferReader extends AsyncIteratorBase<ByteBuffer, AsyncIterator<Uint8Array>> {
    async next(size?: number): Promise<IteratorResult<ByteBuffer>> {
        const r = <any> (await this.source.next(size));
        !r.done && (r.value = new ByteBuffer(r.value));
        return r as IteratorResult<ByteBuffer>;
    }
}

// export class MessageReader<T extends Message> {
//     protected async: boolean;
//     protected source: AsyncIterable<Uint8Array>;
//     constructor(source: AsyncIterable<Uint8Array>) {
//         this.async = isAsyncIterable(this.source = source);
//     }
//     readMessage() { return !this.async ? this._readMessage() : this._readMessageAsync() };
//     _readMessage() {}
//     async _readMessageAsync() {}
// }

const invalidMessageType = (type: MessageHeader) => `Expected ${MessageHeader[type]} message in stream, was null or length 0`;
const noSchemaMessage = () => `Header-pointer of flatbuffer-encoded Message is null.`;

export class RecordBatchReader extends IteratorBase<RecordBatch, MessageReader> {
    public readonly schema: Schema;
    protected dictionaries: Map<number, Vector>;
    constructor(source: MessageReader,
                dictionaries = new Map<number, Vector>()) {
        super(source);
        this.dictionaries = dictionaries;
        this.schema = this._readSchema();
    }
    protected _readSchema() {
        let r: IteratorResult<Message>;
        if ((r = this.source.next()).done) { throw new Error(invalidMessageType(MessageHeader.Schema)); }
        if (r.value.headerType !== MessageHeader.Schema) { throw new Error(noSchemaMessage()); }
        return schemaFromMessage(r.value, new Map());
    }
}
