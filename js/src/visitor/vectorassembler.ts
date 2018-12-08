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

import { Data } from '../data';
import { Vector } from '../vector';
import { Visitor } from '../visitor';
import { Type, UnionMode } from '../enum';
import { RecordBatch } from '../recordbatch';
import { Vector as VType } from '../interfaces';
import { getBool, packBools, iterateBits } from '../util/bit';
import { BufferRegion, FieldNode } from '../ipc/metadata/message';
import {
    DataType, Dictionary,
    Float, Int, Date_, Interval, Time, Timestamp, Union,
    Bool, Null, Utf8, Binary, Decimal, FixedSizeBinary, List, FixedSizeList, Map_, Struct,
} from '../type';

export interface VectorAssembler extends Visitor {
    visitMany <T extends Vector>  (nodes: T[]): this[];
    visit     <T extends Vector>  (node: T   ): this;
    getVisitFn<T extends Type>    (node: T       ): (vector: VType<T>) => this;
    getVisitFn<T extends DataType>(node: VType<T>): (vector: VType<T>) => this;
    getVisitFn<T extends DataType>(node: Data<T> ): (vector: VType<T>) => this;
    getVisitFn<T extends DataType>(node: T       ): (vector: VType<T>) => this;
}

export class VectorAssembler extends Visitor {

    public static assemble<T extends Vector | RecordBatch>(...args: (T | T[])[]) {

        const vectors = args.reduce(function flatten(xs: any[], x: any): any[] {
            if (Array.isArray(x)) { return x.reduce(flatten, xs); }
            if (!(x instanceof RecordBatch)) { return [...xs, x]; }
            return [...xs, ...x.schema.fields.map((_, i) => x.getChildAt(i)!)];
        }, []).filter((x: any): x is Vector => x instanceof Vector);

        return new VectorAssembler().visitMany(vectors)[0];
    }

    private constructor() { super(); }

    public visit<T extends Vector>(vector: T): this {
        if (!DataType.isDictionary(vector.type)) {
            const { data, length, nullCount } = vector;
            if (length > 2147483647) {
                throw new RangeError('Cannot write arrays larger than 2^31 - 1 in length');
            }
            this.addBuffer(nullCount <= 0
                ? new Uint8Array(0) // placeholder validity buffer
                : this.getTruncatedBitmap(data.offset, length, data.nullBitmap)
            ).fieldNodes.push(new FieldNode(length, nullCount));
        }
        return super.visit(vector);
    }

    public visitNull                 <T extends Null>            (_nullV: VType<T>) { return this;                                }
    public visitBool                 <T extends Bool>            (vector: VType<T>) { return     this.assembleBoolVector(vector); }
    public visitInt                  <T extends Int>             (vector: VType<T>) { return     this.assembleFlatVector(vector); }
    public visitFloat                <T extends Float>           (vector: VType<T>) { return     this.assembleFlatVector(vector); }
    public visitUtf8                 <T extends Utf8>            (vector: VType<T>) { return this.assembleFlatListVector(vector); }
    public visitBinary               <T extends Binary>          (vector: VType<T>) { return this.assembleFlatListVector(vector); }
    public visitFixedSizeBinary      <T extends FixedSizeBinary> (vector: VType<T>) { return     this.assembleFlatVector(vector); }
    public visitDate                 <T extends Date_>           (vector: VType<T>) { return     this.assembleFlatVector(vector); }
    public visitTimestamp            <T extends Timestamp>       (vector: VType<T>) { return     this.assembleFlatVector(vector); }
    public visitTime                 <T extends Time>            (vector: VType<T>) { return     this.assembleFlatVector(vector); }
    public visitDecimal              <T extends Decimal>         (vector: VType<T>) { return     this.assembleFlatVector(vector); }
    public visitList                 <T extends List>            (vector: VType<T>) { return     this.assembleListVector(vector); }
    public visitStruct               <T extends Struct>          (vector: VType<T>) { return   this.assembleNestedVector(vector); }
    public visitUnion                <T extends Union>           (vector: VType<T>) { return          this.assembleUnion(vector); }
    public visitDictionary           <T extends Dictionary>      (vector: VType<T>) { return          this.visit(vector.indices); } // Assemble the indices here, Dictionary assembled separately.
    public visitInterval             <T extends Interval>        (vector: VType<T>) { return     this.assembleFlatVector(vector); }
    public visitFixedSizeList        <T extends FixedSizeList>   (vector: VType<T>) { return     this.assembleListVector(vector); }
    public visitMap                  <T extends Map_>            (vector: VType<T>) { return   this.assembleNestedVector(vector); }

    public get buffers() { return this._buffers; }
    public get byteLength() { return this._byteLength; }
    public get fieldNodes() { return this._fieldNodes; }
    public get bufferRegions() { return this._bufferRegions; }

    protected _byteLength = 0;
    protected _fieldNodes: FieldNode[] = [];
    protected _buffers: ArrayBufferView[] = [];
    protected _bufferRegions: BufferRegion[] = [];

    protected addBuffer(values: ArrayBufferView) {
        const byteLength = (values.byteLength + 7) & ~7; // Round up to a multiple of 8
        this.buffers.push(values);
        this.bufferRegions.push(new BufferRegion(this._byteLength, byteLength));
        this._byteLength += byteLength;
        return this;
    }
    protected assembleUnion<T extends Union>(this: VectorAssembler, vector: VType<T>) {
        const { type, length, typeIds } = vector;
        // All Union Vectors have a typeIds buffer
        this.addBuffer(typeIds);
        // If this is a Sparse Union, treat it like all other Nested types
        if (type.mode === UnionMode.Sparse) {
            return this.assembleNestedVector(vector);
        } else if (type.mode === UnionMode.Dense) {
            // If this is a Dense Union, add the valueOffsets buffer and potentially slice the children
            if (vector.offset <= 0) {
                // If the Vector hasn't been sliced, write the existing valueOffsets
                this.addBuffer(vector.valueOffsets);
                // We can treat this like all other Nested types
                return this.assembleNestedVector(vector);
            } else {
                // A sliced Dense Union is an unpleasant case. Because the offsets are different for
                // each child vector, we need to "rebase" the valueOffsets for each child
                // Union typeIds are not necessary 0-indexed
                const maxChildTypeId = typeIds.reduce((x, y) => Math.max(x, y), typeIds[0]);
                const childLengths = new Int32Array(maxChildTypeId + 1);
                // Set all to -1 to indicate that we haven't observed a first occurrence of a particular child yet
                const childOffsets = new Int32Array(maxChildTypeId + 1).fill(-1);
                const shiftedOffsets = new Int32Array(length);
                // If we have a non-zero offset, then the value offsets do not start at
                // zero. We must a) create a new offsets array with shifted offsets and
                // b) slice the values array accordingly
                const unshiftedOffsets = this.getZeroBasedValueOffsets(0, length, vector.valueOffsets);
                for (let typeId, shift, index = -1; ++index < length;) {
                    if ((shift = childOffsets[typeId = typeIds[index]]) === -1) {
                        shift = childOffsets[typeId] = unshiftedOffsets[typeId];
                    }
                    shiftedOffsets[index] = unshiftedOffsets[index] - shift;
                    ++childLengths[typeId];
                }
                this.addBuffer(shiftedOffsets);
                // Slice and visit children accordingly
                for (let child: Vector | null, childIndex = -1, numChildren = type.children.length; ++childIndex < numChildren;) {
                    if (child = vector.getChildAt(childIndex)) {
                        const typeId = type.typeIds[childIndex];
                        const childLength = Math.min(length, childLengths[typeId]);
                        this.visit(child.slice(childOffsets[typeId], childLength));
                    }
                }
            }
        }
        return this;
    }
    protected assembleBoolVector<T extends Bool>(this: VectorAssembler, vector: VType<T>) {
        // Bool vector is a special case of FlatVector, as its data buffer needs to stay packed
        let values: Uint8Array;
        if (vector.nullCount >= vector.length) {
            // If all values are null, just insert a placeholder empty data buffer (fastest path)
            return this.addBuffer(new Uint8Array(0));
        } else if ((values = vector.values) instanceof Uint8Array) {
            // If values is already a Uint8Array, slice the bitmap (fast path)
            return this.addBuffer(this.getTruncatedBitmap(vector.offset, vector.length, values));
        }
        // Otherwise if the underlying data *isn't* a Uint8Array, enumerate
        // the values as bools and re-pack them into a Uint8Array (slow path)
        return this.addBuffer(packBools(vector));
    }
    protected assembleFlatVector<T extends Int | Float | FixedSizeBinary | Date_ | Timestamp | Time | Decimal | Interval>(this: VectorAssembler, vector: VType<T>) {
        return this.addBuffer(vector.values.subarray(0, vector.length * vector.stride));
    }
    protected assembleFlatListVector<T extends Utf8 | Binary>(this: VectorAssembler, vector: VType<T>) {
        const { length, values, valueOffsets } = vector;
        const firstOffset = valueOffsets[0];
        const lastOffset = valueOffsets[length];
        const byteLength = Math.min(lastOffset - firstOffset, values.byteLength - firstOffset);
        // Push in the order FlatList types read their buffers
        return this
            .addBuffer(this.getZeroBasedValueOffsets(0, length, valueOffsets)) // valueOffsets buffer first
            .addBuffer(values.subarray(firstOffset, firstOffset + byteLength)) // sliced values buffer second
            ;
    }
    protected assembleListVector<T extends List | FixedSizeList>(this: VectorAssembler, vector: VType<T>) {
        const { length, valueOffsets } = vector;
        // If we have valueOffsets (ListVector), push that buffer first
        if (valueOffsets) {
            this.addBuffer(this.getZeroBasedValueOffsets(0, length, valueOffsets));
        }
        // Then insert the List's values child
        return this.visit(vector.getChildAt(0)!);
    }
    protected assembleNestedVector<T extends Struct | Map_ | Union>(this: VectorAssembler, vector: VType<T>) {
        return this.visitMany(vector.type.children.map((_, i) => vector.getChildAt(i)!).filter(Boolean))[0];
    }
    protected getTruncatedBitmap(offset: number, length: number, bitmap: Uint8Array) {
        // If the offset is a multiple of 8 bits, it's safe to slice the bitmap
        return (offset % 8 === 0) ? bitmap.subarray(offset >> 3)
            // Otherwise iterate each bit from the offset and return a new one
            : packBools(iterateBits(bitmap, offset, length, null, getBool));
    }
    protected getZeroBasedValueOffsets(offset: number, length: number, valueOffsets: Int32Array) {
        // If we have a non-zero offset, create a new offsets array with the values
        // shifted by the start offset, such that the new start offset is 0
        if (offset > 0 || valueOffsets[0] !== 0) {
            const start = valueOffsets[0];
            valueOffsets = valueOffsets.slice(0, length + 1);
            for (let i = -1; ++i <= length;) {
                valueOffsets[i] -= start;
            }
        }
        return valueOffsets;
    }
}

VectorAssembler.prototype.visitBool            =     (VectorAssembler.prototype as any).assembleBoolVector;
VectorAssembler.prototype.visitInt             =     (VectorAssembler.prototype as any).assembleFlatVector;
VectorAssembler.prototype.visitFloat           =     (VectorAssembler.prototype as any).assembleFlatVector;
VectorAssembler.prototype.visitUtf8            = (VectorAssembler.prototype as any).assembleFlatListVector;
VectorAssembler.prototype.visitBinary          = (VectorAssembler.prototype as any).assembleFlatListVector;
VectorAssembler.prototype.visitFixedSizeBinary =     (VectorAssembler.prototype as any).assembleFlatVector;
VectorAssembler.prototype.visitDate            =     (VectorAssembler.prototype as any).assembleFlatVector;
VectorAssembler.prototype.visitTimestamp       =     (VectorAssembler.prototype as any).assembleFlatVector;
VectorAssembler.prototype.visitTime            =     (VectorAssembler.prototype as any).assembleFlatVector;
VectorAssembler.prototype.visitDecimal         =     (VectorAssembler.prototype as any).assembleFlatVector;
VectorAssembler.prototype.visitList            =     (VectorAssembler.prototype as any).assembleListVector;
VectorAssembler.prototype.visitStruct          =   (VectorAssembler.prototype as any).assembleNestedVector;
VectorAssembler.prototype.visitUnion           =          (VectorAssembler.prototype as any).assembleUnion;
VectorAssembler.prototype.visitInterval        =     (VectorAssembler.prototype as any).assembleFlatVector;
VectorAssembler.prototype.visitFixedSizeList   =     (VectorAssembler.prototype as any).assembleListVector;
VectorAssembler.prototype.visitMap             =   (VectorAssembler.prototype as any).assembleNestedVector;
