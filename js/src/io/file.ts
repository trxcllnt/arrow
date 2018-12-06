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

import { FileHandle } from './interfaces';
import { ByteSource, AsyncByteSource } from './stream';

/**
 * @ignore
 */
export class RandomAccessFile extends ByteSource {
    public size: number;
    public position: number = 0;
    protected buffer: Uint8Array | null;
    constructor(buffer: Uint8Array, byteLength = buffer.byteLength) {
        super();
        this.buffer = buffer;
        this.size = byteLength;
    }
    public readInt32(position: number) {
        const { buffer, byteOffset } = this.readAt(position, 4);
        return new DataView(buffer, byteOffset).getInt32(0, true);
    }
    public seek(position: number) {
        this.position = Math.min(position, this.size);
        return position < this.size;
    }
    public read(nBytes?: number | null) {
        const { buffer, size, position } = this;
        if (buffer && position < size) {
            if (typeof nBytes !== 'number') { nBytes = Infinity; }
            this.position = Math.min(size,
                 position + Math.min(size - position, nBytes));
            return buffer.subarray(position, this.position);
        }
        return null;
    }
    public readAt(position: number, nBytes: number) {
        const buf = this.buffer;
        const end = Math.min(this.size, position + nBytes);
        return buf ? buf.subarray(position, end) : new Uint8Array(nBytes);
    }
    public close() { this.buffer && (this.buffer = null); }
    public throw(value?: any) { this.close(); return { done: true, value }; }
    public return(value?: any) { this.close(); return { done: true, value }; }
}

/**
 * @ignore
 */
export class AsyncRandomAccessFile extends AsyncByteSource {
    public size: number;
    public position: number = 0;
    protected file: FileHandle | null;
    constructor(file: FileHandle, byteLength: number) {
        super();
        this.file = file;
        this.size = byteLength;
    }
    public async readInt32(position: number) {
        const { buffer, byteOffset } = await this.readAt(position, 4);
        return new DataView(buffer, byteOffset).getInt32(0, true);
    }
    public async seek(position: number) {
        this.position = Math.min(position, this.size);
        return position < this.size;
    }
    public async read(nBytes?: number | null) {
        const { file, size, position } = this;
        if (file && position < size) {
            if (typeof nBytes !== 'number') { nBytes = Infinity; }
            let pos = position, offset = 0, bytesRead = 0;
            let end = Math.min(size, pos + Math.min(size - pos, nBytes));
            let buffer = new Uint8Array(Math.max(0, (this.position = end) - pos));
            while ((pos += bytesRead) < end && (offset += bytesRead) < buffer.byteLength) {
                ({ bytesRead } = await file.read(buffer, offset, buffer.byteLength - offset, pos));
            }
            return buffer;
        }
        return null;
    }
    public async readAt(position: number, nBytes: number) {
        const { file, size } = this;
        if (file && (position + nBytes) < size) {
            const end = Math.min(size, position + nBytes);
            const buffer = new Uint8Array(end - position);
            return (await file.read(buffer, 0, nBytes, position)).buffer;
        }
        return new Uint8Array(nBytes);
    }
    public async close() { const f = this.file; this.file = null; f && await f.close(); }
    public async throw(value?: any) { await this.close(); return { done: true, value }; }
    public async return(value?: any) { await this.close(); return { done: true, value }; }
}
