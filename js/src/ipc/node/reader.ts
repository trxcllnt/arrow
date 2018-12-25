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

import { Duplex } from 'stream';
import { DataType } from '../../type';
import { RecordBatch } from '../../recordbatch';
import { AsyncByteQueue } from '../../io/stream';
import { RecordBatchReader } from '../../ipc/reader';

export function recordBatchReaderThroughNodeStream<T extends { [key: string]: DataType } = any>() {
    return new RecordBatchReaderDuplex<T>();
}

type CB = (error?: Error | null | undefined) => void;

class RecordBatchReaderDuplex<T extends { [key: string]: DataType } = any> extends Duplex {
    private _pulling: boolean = false;
    private _reader: RecordBatchReader | null;
    private _asyncQueue: AsyncByteQueue | null;
    constructor() {
        super({ allowHalfOpen: false, readableObjectMode: true, writableObjectMode: false });
        this._reader = null;
        this._asyncQueue = new AsyncByteQueue();
    }
    _final(cb?: CB) {
        const aq = this._asyncQueue;
        aq && aq.close();
        cb && cb();
    }
    _write(x: any, _: string, cb: CB) {
        const aq = this._asyncQueue;
        aq && aq.write(x);
        cb && cb();
        return true;
    }
    _read(size: number) {
        const aq = this._asyncQueue;
        if (aq && !this._pulling && (this._pulling = true)) {
            (async () => {
                if (!this._reader) {
                    this._reader = await this._open(aq);
                };
                this._pulling = await this._pull(size, this._reader);
            })();
        }
    }
    _destroy(err: Error | null, cb: (error: Error | null) => void) {
        const aq = this._asyncQueue;
        if (aq) { err ? aq.abort(err) : aq.close(); }
        cb(this._asyncQueue = this._reader = null);
    }
    async _open(source: AsyncByteQueue) {
        return await (await RecordBatchReader.from(source)).open();
    }
    async _pull(size: number, reader: RecordBatchReader<T>) {
        let r: IteratorResult<RecordBatch<T>> | null = null;
        while (this.readable && !(r = await reader.next()).done) {
            if (!this.push(r.value) || (size != null && --size <= 0)) { break; }
        }
        if ((r && r.done || !this.readable) && (this.push(null) || true)) {
            await reader.cancel();
        }
        return !this.readable;
    }
}
