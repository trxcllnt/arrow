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

import { DataType } from './type';
import { popcnt_bit_range } from './util/bit';
import { ArrayBufferViewConstructor } from './type';
import { Type, VectorType as BufferType, UnionMode } from './enum';
import {
    Dictionary,
    Null, Int, Float,
    Binary, Bool, Utf8, Decimal,
    Date_, Time, Timestamp, Interval,
    List, Struct, Union, FixedSizeBinary, FixedSizeList, Map_,
} from './type';

// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Array::null_count is called the
// first time, the null count will be computed. See ARROW-33
export type kUnknownNullCount = -1;
export const kUnknownNullCount = -1;

export type NullBuffer = Uint8Array | null | undefined;
export type ValueOffsetsBuffer = Int32Array | ArrayLike<number> | Iterable<number>;
export type DataBuffer<T extends DataType> = T['TArray'] | ArrayLike<number> | Iterable<number>;

export interface Buffers<T extends DataType> {
      [BufferType.OFFSET]?: Int32Array;
        [BufferType.DATA]?: T['TArray'];
    [BufferType.VALIDITY]?: Uint8Array;
        [BufferType.TYPE]?: T['TArray'];
};

export class Data<T extends DataType = DataType> {

    public type: T;
    public readonly length: number;
    public readonly offset: number;
    
    // @ts-ignore
    public readonly childData: Data[];
    protected _buffers = [] as Buffers<T>;
    protected _nullCount: number | kUnknownNullCount;

    public get TType() { return this.type.TType; }
    public get TArray() { return this.type.TArray; };
    public get TValue() { return this.type.TValue; };
    public get ArrayType() { return this.type.ArrayType; };

    public get values() { return this._buffers[BufferType.DATA]!; }
    public get typeIds() { return this._buffers[BufferType.TYPE]!; }
    public get nullBitmap() { return this._buffers[BufferType.VALIDITY]!; }
    public get valueOffsets() { return this._buffers[BufferType.OFFSET]!; }
    public get nullCount() {
        let nullCount = this._nullCount;
        let nullBitmap: Uint8Array | undefined;
        if (nullCount === kUnknownNullCount && (nullBitmap = this.nullBitmap)) {
            this._nullCount = nullCount = this.length - popcnt_bit_range(nullBitmap, this.offset, this.offset + this.length);
        }
        return nullCount;
    }

    constructor(type: T, offset: number, length: number, nullCount?: number, buffers?: Buffers<T>, childData?: Data[]) {
        this.type = type;
        this.childData = childData!;
        this.offset = Math.floor(Math.max(offset || 0, 0));
        this.length = Math.floor(Math.max(length || 0, 0));
        this._buffers = Object.assign([], buffers) as Buffers<T>;
        this._nullCount = Math.floor(Math.max(nullCount || 0, -1));
    }

    public clone<R extends DataType>(type: R, offset = this.offset, length = this.length, nullCount = this._nullCount, buffers: Buffers<R> = <any> this._buffers, childData: Data[] = this.childData) {
        return new Data(type, offset, length, nullCount, buffers, childData);
    }

    public slice(offset: number, length: number): Data<T> {
        // +true === 1, +false === 0, so this means
        // we keep nullCount at 0 if it's already 0,
        // otherwise set to the invalidated flag -1
        const nullCount = +(this._nullCount === 0) - 1;
        const buffers = this.sliceBuffers(offset, length);
        const childData = this.sliceChildren(offset, length);
        return this.clone<T>(this.type, this.offset + offset, length, nullCount, buffers, childData);
    }

    protected sliceBuffers(offset: number, length: number): Buffers<T> {
        let arr: any, buffers = {} as Buffers<T>;
        // If typeIds exist, slice the typeIds buffer
        (arr = this.typeIds) && (buffers[BufferType.TYPE] = this.sliceData(arr, offset, length));
        // If offsets exist, only slice the offsets buffer
        (arr = this.valueOffsets) && (buffers[BufferType.OFFSET] = this.sliceOffsets(arr, offset, length)) ||
            // Otherwise if no offsets, slice the data buffer
            (arr = this.values) && (buffers[BufferType.DATA] = this.sliceData(arr, offset, length));
        return buffers;
    }
    protected sliceChildren(offset: number, length: number): Data[] {
        // Only slice children if this isn't variable width data
        if (!this.valueOffsets) {
            return this.childData.map((child) => child.slice(offset, length));
        }
        return this.childData;
    }
    protected sliceData(data: T['TArray'] & ArrayBufferView, offset: number, length: number) {
        // Don't slice the data vector for Booleans, since the offset goes by bits not bytes
        return this.type.TType === Type.Bool ? data : data.subarray(offset, offset + length);
    }
    protected sliceOffsets(valueOffsets: Int32Array, offset: number, length: number) {
        return valueOffsets.subarray(offset, offset + length + 1);
    }

    // Convenience methods for creating Data instances for each of the Arrow Vector types
    public static Null           <T extends Null           >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap) }); }
    public static Int            <T extends Int            >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static Dictionary     <T extends Dictionary     >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static Float          <T extends Float          >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static Bool           <T extends Bool           >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static Decimal        <T extends Decimal        >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static Date           <T extends Date_          >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static Time           <T extends Time           >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static Timestamp      <T extends Timestamp      >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static Interval       <T extends Interval       >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static FixedSizeBinary<T extends FixedSizeBinary>(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: DataBuffer<T>) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.DATA]: toTypedArray(type.ArrayType, data) }); }
    public static Binary         <T extends Binary         >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: Uint8Array, valueOffsets: ValueOffsetsBuffer) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.OFFSET]: toTypedArray(Int32Array, valueOffsets), [BufferType.DATA]: toTypedArray(Uint8Array, data) }); }
    public static Utf8           <T extends Utf8           >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, data: Uint8Array, valueOffsets: ValueOffsetsBuffer) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.OFFSET]: toTypedArray(Int32Array, valueOffsets), [BufferType.DATA]: toTypedArray(Uint8Array, data) }); }
    public static List           <T extends List           >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, valueOffsets: ValueOffsetsBuffer, childData: Data[]) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap), [BufferType.OFFSET]: toTypedArray(Int32Array, valueOffsets) }, childData); }
    public static FixedSizeList  <T extends FixedSizeList  >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, childData: Data[]) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap) }, childData); }
    public static Struct         <T extends Struct         >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, childData: Data[]) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap) }, childData); }
    public static Map            <T extends Map_           >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, childData: Data[]) { return new Data(type, offset, length, nullCount, { [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap) }, childData); }
    public static Union          <T extends Union          >(type: T, offset: number, length: number, nullCount: number, nullBitmap: NullBuffer, valueOffsets: ValueOffsetsBuffer, typeIds: Uint8Array, childData: Data[]) {
        const buffers = {
            [BufferType.VALIDITY]: toTypedArray(Uint8Array, nullBitmap),
            [BufferType.TYPE]: toTypedArray(type.ArrayType, typeIds)
        } as any;
        if (type.mode === UnionMode.Dense) {
            buffers[BufferType.OFFSET] = toTypedArray(Int32Array, valueOffsets);
        }
        return new Data(type, offset, length, nullCount, buffers, childData);
    }
}

export function toTypedArray<T extends ArrayBufferView>(ArrayType: ArrayBufferViewConstructor<T>, values?: T | ArrayLike<number> | Iterable<number> | null): T {
    if (!ArrayType && ArrayBuffer.isView(values)) { return values; }
    return values instanceof ArrayType ? values
         : !values || !ArrayBuffer.isView(values) ? ArrayType.from(values || [])
         : new ArrayType(values.buffer, values.byteOffset, values.byteLength / ArrayType.BYTES_PER_ELEMENT);
}
