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

import {
    Schema,
    RecordBatch,
    MessageReader,
    RecordBatchReader,
    AsyncMessageReader,
    RecordBatchFileReader,
    RecordBatchStreamReader
} from '../../Arrow';

export function simpleStreamSyncMessageReaderTest(reader: MessageReader) {
    let index = 0;
    for (let message of reader) {
        if (index++ === 0) {
            expect(message.isSchema()).toBe(true);
            expect(message.bodyLength).toBe(0);
        } else {
            expect(message.isRecordBatch()).toBe(true);
        }
        const body = reader.readMessageBody(message.bodyLength);
        expect(body).toBeInstanceOf(Uint8Array);
        expect(body.byteLength).toBe(message.bodyLength);
    }
    reader.return();
    return reader;
}

export async function simpleStreamAsyncMessageReaderTest(reader: AsyncMessageReader) {
    let index = 0;
    for await (let message of reader) {
        if (index++ === 0) {
            expect(message.isSchema()).toBe(true);
            expect(message.bodyLength).toBe(0);
        } else {
            expect(message.isRecordBatch()).toBe(true);
        }
        const body = await reader.readMessageBody(message.bodyLength);
        expect(body).toBeInstanceOf(Uint8Array);
        expect(body.byteLength).toBe(message.bodyLength);
    }
    await reader.return();
    return reader;
}

export function testSimpleRecordBatchJSONReader(r: RecordBatchStreamReader) {
    const reader = r.open();
    expect(reader.isStream()).toBe(true);
    expect(reader.schema).toBeInstanceOf(Schema);
    testSimpleRecordBatchIterator(reader[Symbol.iterator]());
    expect(reader.closed).toBe(reader.autoClose);
    return reader;
}

export function testSimpleRecordBatchFileReader<T extends RecordBatchFileReader | RecordBatchStreamReader>(r: T) {
    const reader = r.open();
    expect(reader.isFile()).toBe(true);
    expect(reader.schema).toBeInstanceOf(Schema);
    testSimpleRecordBatchIterator(reader[Symbol.iterator]());
    expect(reader.closed).toBe(reader.autoClose);
    return reader;
}

export function testSimpleRecordBatchStreamReader<T extends RecordBatchFileReader | RecordBatchStreamReader>(r: T) {
    const reader = r.open();
    expect(reader.isStream()).toBe(true);
    expect(reader.schema).toBeInstanceOf(Schema);
    testSimpleRecordBatchIterator(reader[Symbol.iterator]());
    expect(reader.closed).toBe(reader.autoClose);
    return reader;
}

export async function testSimpleAsyncRecordBatchFileReader<T extends RecordBatchReader>(r: T) {
    const reader = await r.open();
    expect(reader.isFile()).toBe(true);
    expect(reader.schema).toBeInstanceOf(Schema);
    await testSimpleAsyncRecordBatchIterator(reader[Symbol.asyncIterator]());
    expect(reader.closed).toBe(reader.autoClose);
    return reader;
}

export async function testSimpleAsyncRecordBatchStreamReader<T extends RecordBatchReader>(r: T) {
    const reader = await r.open();
    expect(reader.isStream()).toBe(true);
    expect(reader.schema).toBeInstanceOf(Schema);
    await testSimpleAsyncRecordBatchIterator(reader[Symbol.asyncIterator]());
    expect(reader.closed).toBe(reader.autoClose);
    return reader;
}

export function testSimpleRecordBatchIterator(iterator: IterableIterator<RecordBatch>) {
    let i = 0;
    for (const recordBatch of iterator) {
        i++;
        try {
            expect(recordBatch).toBeInstanceOf(RecordBatch);
        } catch (e) { throw new Error(`${i}: ${e}`); }
    }
    expect(i).toBe(3);
    iterator.return!();
}

export async function testSimpleAsyncRecordBatchIterator(iterator: AsyncIterableIterator<RecordBatch>) {
    let i = 0;
    for await (const recordBatch of iterator) {
        i++;
        try {
            expect(recordBatch).toBeInstanceOf(RecordBatch);
        } catch (e) { throw new Error(`${i}: ${e}`); }
    }
    expect(i).toBe(3);
    await iterator.return!();
}
