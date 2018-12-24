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
import { AsyncByteStream } from '../../io/stream';
import { RecordBatchWriter } from '../../ipc/writer';

export function recordBatchWriterThroughNodeStream<T extends { [key: string]: DataType } = any>(this: typeof RecordBatchWriter) {
    return new RecordBatchWriterDuplex(new this<T>());
}

type CB = (error?: Error | null | undefined) => void;

class RecordBatchWriterDuplex<T extends { [key: string]: DataType } = any> extends Duplex {
    private _pulling: boolean = false;
    private _reader: AsyncByteStream | null;
    private _writer: RecordBatchWriter | null;
    constructor(writer: RecordBatchWriter<T>) {
        super({ allowHalfOpen: false, writableObjectMode: true, readableObjectMode: false });
        this._writer = writer;
        this._reader = new AsyncByteStream(writer);
    }
    _final(cb?: CB) {
        const writer = this._writer;
        if (writer) { writer.close(); }
        if (cb) { cb(); }
    }
    _write(x: any, _: string, cb: CB) {
        const writer = this._writer;
        if (writer) { writer.write(x); }
        if (cb) { cb(); }
        return true;
    }
    _writev(xs: { chunk: any, encoding: string }[], cb: CB) {
        const writer = this._writer;
        if (writer) { xs.forEach(({ chunk }) => writer.write(chunk)); }
        if (cb) { cb(); }
    }
    _read(size: number) {
        const it = this._reader;
        if (it && !this._pulling && (this._pulling = true)) {
            (async () => this._pulling = await this._pull(size, it))();
        }
    }
    _destroy(err: Error | null, cb: (error: Error | null) => void) {
        const writer = this._writer;
        const reader = this._reader;
        if (reader) { reader.cancel(err); }
        if (writer) { err ? writer.abort(err) : writer.close(); }
        cb(this._reader = this._writer = null);
    }
    async _pull(size: number, reader: AsyncByteStream) {
        let r: IteratorResult<Uint8Array> | null = null;
        while (this.readable && !(r = await reader.next(size || null)).done) {
            if (size != null && r.value) {
                size -= r.value.byteLength;
            }
            if (!this.push(r.value) || size <= 0) { break; }
        }
        if ((r && r.done || !this.readable) && (this.push(null) || true)) {
            await reader.cancel();
        }
        return !this.readable;
    }
}
