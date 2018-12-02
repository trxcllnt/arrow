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

import { ArrowJSON } from '../input';
import { MessageHeader } from '../../enum';
import { Message } from '../metadata/message';
import { MessageReader } from './messagereader';
import { ITERATOR_DONE } from '../../io/interfaces';
import { nullMessage, invalidMessageType } from './support';

export class JSONMessageReader extends MessageReader {
    private _schema = false;
    private _body: any[] = [];
    private _batchIndex = 0;
    private _dictionaryIndex = 0;
    constructor(private _json: ArrowJSON) {
        super(null as any);
    }
    public next() {
        const { _json, _batchIndex, _dictionaryIndex } = this;
        const numBatches = _json.batches.length;
        const numDictionaries = _json.dictionaries.length;
        if (!this._schema) {
            this._schema = true;
            const message = Message.fromJSON(_json.schema, MessageHeader.Schema);
            return { value: message, done: _batchIndex >= numBatches && _dictionaryIndex >= numDictionaries };
        }
        if (_dictionaryIndex < numDictionaries) {
            const batch = _json.dictionaries[this._dictionaryIndex++];
            this._body = batch['data']['columns'];
            const message = Message.fromJSON(batch, MessageHeader.DictionaryBatch);
            return { done: false, value: message };
        }
        if (_batchIndex < numBatches) {
            const batch = _json.batches[this._batchIndex++];
            this._body = batch['columns'];
            const message = Message.fromJSON(batch, MessageHeader.RecordBatch);
            return { done: false, value: message };
        }
        this._body = [];
        return ITERATOR_DONE;
    }
    public readMessageBody(_bodyLength?: number) {
        return flattenDataSources(this._body) as any;
        function flattenDataSources(xs: any[]): any[][] {
            return (xs || []).reduce<any[][]>((buffers, column: any) => [
                ...buffers,
                ...(column['VALIDITY'] && [column['VALIDITY']] || []),
                ...(column['OFFSET'] && [column['OFFSET']] || []),
                ...(column['TYPE'] && [column['TYPE']] || []),
                ...(column['DATA'] && [column['DATA']] || []),
                ...flattenDataSources(column['children'])
            ], [] as any[][]);
        }
    }
    public readMessage<T extends MessageHeader>(type?: T | null) {
        let r: IteratorResult<Message<T>>;
        if ((r = this.next()).done) { return null; }
        if ((type != null) && r.value.headerType !== type) {
            throw new Error(invalidMessageType(type));
        }
        return r.value;
    }
    public readSchema() {
        const type = MessageHeader.Schema;
        const message = this.readMessage(type);
        const schema = message && message.header();
        if (!message || !schema) {
            throw new Error(nullMessage(type));
        }
        return schema;
    }
}
