// import { Vector } from '../interfaces';
// import { flatbuffers } from 'flatbuffers';
// import { Schema, Long } from '../schema';
// import { RecordBatch } from '../recordbatch';
// import { isAsyncIterable } from '../util/compat';
// import { Message, DictionaryBatch, RecordBatchMetadata } from '../ipc/metadata';

import { RecordBatch } from '../recordbatch';
import { Message, MessageReader } from './message';
import { Schema } from '../schema';
import { Vector } from '../interfaces';
import { MessageHeader } from '../enum';
import * as internal from './metadata-internal';
import {
    IteratorBase, // AsyncIteratorBase
} from './stream';

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
        if ((r = this.source.next()).done) {
            throw new Error(invalidMessageType(MessageHeader.Schema));
        }
        if (r.value.headerType !== MessageHeader.Schema) {
            throw new Error(noSchemaMessage());
        }
        return internal.getSchema(r.value, new Map());
    }
}
