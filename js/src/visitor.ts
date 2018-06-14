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
    Type, DataType, Dictionary,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Union, Map_, Struct,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp
} from './type';

import {
    Vector, DictionaryVector,
    NullVector, BoolVector, IntVector, FloatVector, DecimalVector,
    Utf8Vector, BinaryVector, FixedSizeBinaryVector,
    DateVector, TimestampVector, TimeVector, IntervalVector,
    ListVector, FixedSizeListVector, StructVector, UnionVector, MapVector
} from './vector';

export interface VisitorNode {
    acceptTypeVisitor(visitor: TypeVisitor): any;
    acceptVectorVisitor(visitor: VectorVisitor): any;
    acceptOperatorVisitor<T extends DataType>(visitor: OperatorVisitor, index: number, value: T['TValue'] | null): any;
}

export abstract class TypeVisitor {
    visit(type: Partial<VisitorNode>): any {
        return type.acceptTypeVisitor && type.acceptTypeVisitor(this) || null;
    }
    visitMany(types: Partial<VisitorNode>[]): any[] {
        return types.map((type) => this.visit(type));
    }
    abstract visitNull?(type: Null): any;
    abstract visitBool?(type: Bool): any;
    abstract visitInt?(type: Int): any;
    abstract visitFloat?(type: Float): any;
    abstract visitUtf8?(type: Utf8): any;
    abstract visitBinary?(type: Binary): any;
    abstract visitFixedSizeBinary?(type: FixedSizeBinary): any;
    abstract visitDate?(type: Date_): any;
    abstract visitTimestamp?(type: Timestamp): any;
    abstract visitTime?(type: Time): any;
    abstract visitDecimal?(type: Decimal): any;
    abstract visitList?(type: List): any;
    abstract visitStruct?(type: Struct): any;
    abstract visitUnion?(type: Union<any>): any;
    abstract visitDictionary?(type: Dictionary): any;
    abstract visitInterval?(type: Interval): any;
    abstract visitFixedSizeList?(type: FixedSizeList): any;
    abstract visitMap?(type: Map_): any;

    static visitTypeInline<T extends DataType>(visitor: TypeVisitor, type: T): any {
        switch (type.TType) {
            case Type.Null:            return visitor.visitNull            && visitor.visitNull(type            as any as Null);
            case Type.Int:             return visitor.visitInt             && visitor.visitInt(type             as any as Int);
            case Type.Float:           return visitor.visitFloat           && visitor.visitFloat(type           as any as Float);
            case Type.Binary:          return visitor.visitBinary          && visitor.visitBinary(type          as any as Binary);
            case Type.Utf8:            return visitor.visitUtf8            && visitor.visitUtf8(type            as any as Utf8);
            case Type.Bool:            return visitor.visitBool            && visitor.visitBool(type            as any as Bool);
            case Type.Decimal:         return visitor.visitDecimal         && visitor.visitDecimal(type         as any as Decimal);
            case Type.Date:            return visitor.visitDate            && visitor.visitDate(type            as any as Date_);
            case Type.Time:            return visitor.visitTime            && visitor.visitTime(type            as any as Time);
            case Type.Timestamp:       return visitor.visitTimestamp       && visitor.visitTimestamp(type       as any as Timestamp);
            case Type.Interval:        return visitor.visitInterval        && visitor.visitInterval(type        as any as Interval);
            case Type.List:            return visitor.visitList            && visitor.visitList(type            as any as List<T>);
            case Type.Struct:          return visitor.visitStruct          && visitor.visitStruct(type          as any as Struct);
            case Type.Union:           return visitor.visitUnion           && visitor.visitUnion(type           as any as Union);
            case Type.FixedSizeBinary: return visitor.visitFixedSizeBinary && visitor.visitFixedSizeBinary(type as any as FixedSizeBinary);
            case Type.FixedSizeList:   return visitor.visitFixedSizeList   && visitor.visitFixedSizeList(type   as any as FixedSizeList);
            case Type.Map:             return visitor.visitMap             && visitor.visitMap(type             as any as Map_);
            case Type.Dictionary:      return visitor.visitDictionary      && visitor.visitDictionary(type      as any as Dictionary);
            default: return null;
        }
    }
}

export abstract class VectorVisitor {
    visit(vector: Partial<VisitorNode>): any {
        return vector.acceptVectorVisitor && vector.acceptVectorVisitor(this) || null;
    }
    visitMany(vectors: Partial<VisitorNode>[]): any[] {
        return vectors.map((vector) => this.visit(vector));
    }
    abstract visitNull?(vector: Vector<Null>): any;
    abstract visitBool?(vector: Vector<Bool>): any;
    abstract visitInt?(vector: Vector<Int>): any;
    abstract visitFloat?(vector: Vector<Float>): any;
    abstract visitUtf8?(vector: Vector<Utf8>): any;
    abstract visitBinary?(vector: Vector<Binary>): any;
    abstract visitFixedSizeBinary?(vector: Vector<FixedSizeBinary>): any;
    abstract visitDate?(vector: Vector<Date_>): any;
    abstract visitTimestamp?(vector: Vector<Timestamp>): any;
    abstract visitTime?(vector: Vector<Time>): any;
    abstract visitDecimal?(vector: Vector<Decimal>): any;
    abstract visitList?(vector: Vector<List>): any;
    abstract visitStruct?(vector: Vector<Struct>): any;
    abstract visitUnion?(vector: Vector<Union<any>>): any;
    abstract visitDictionary?(vector: Vector<Dictionary>): any;
    abstract visitInterval?(vector: Vector<Interval>): any;
    abstract visitFixedSizeList?(vector: Vector<FixedSizeList>): any;
    abstract visitMap?(vector: Vector<Map_>): any;

    static visitTypeInline<T extends DataType>(visitor: VectorVisitor, type: T, vector: Vector<T>): any {
        switch (type.TType) {
            case Type.Null:            return visitor.visitNull            && visitor.visitNull(vector            as any as Vector<Null>);
            case Type.Int:             return visitor.visitInt             && visitor.visitInt(vector             as any as Vector<Int>);
            case Type.Float:           return visitor.visitFloat           && visitor.visitFloat(vector           as any as Vector<Float>);
            case Type.Binary:          return visitor.visitBinary          && visitor.visitBinary(vector          as any as Vector<Binary>);
            case Type.Utf8:            return visitor.visitUtf8            && visitor.visitUtf8(vector            as any as Vector<Utf8>);
            case Type.Bool:            return visitor.visitBool            && visitor.visitBool(vector            as any as Vector<Bool>);
            case Type.Decimal:         return visitor.visitDecimal         && visitor.visitDecimal(vector         as any as Vector<Decimal>);
            case Type.Date:            return visitor.visitDate            && visitor.visitDate(vector            as any as Vector<Date_>);
            case Type.Time:            return visitor.visitTime            && visitor.visitTime(vector            as any as Vector<Time>);
            case Type.Timestamp:       return visitor.visitTimestamp       && visitor.visitTimestamp(vector       as any as Vector<Timestamp>);
            case Type.Interval:        return visitor.visitInterval        && visitor.visitInterval(vector        as any as Vector<Interval>);
            case Type.List:            return visitor.visitList            && visitor.visitList(vector            as any as Vector<List<T>>);
            case Type.Struct:          return visitor.visitStruct          && visitor.visitStruct(vector          as any as Vector<Struct>);
            case Type.Union:           return visitor.visitUnion           && visitor.visitUnion(vector           as any as Vector<Union>);
            case Type.FixedSizeBinary: return visitor.visitFixedSizeBinary && visitor.visitFixedSizeBinary(vector as any as Vector<FixedSizeBinary>);
            case Type.FixedSizeList:   return visitor.visitFixedSizeList   && visitor.visitFixedSizeList(vector   as any as Vector<FixedSizeList>);
            case Type.Map:             return visitor.visitMap             && visitor.visitMap(vector             as any as Vector<Map_>);
            case Type.Dictionary:      return visitor.visitDictionary      && visitor.visitDictionary(vector      as any as Vector<Dictionary>);
            default: return null;
        }
    }
}

export abstract class OperatorVisitor {
    visit<T extends DataType>(vector: Partial<VisitorNode>, index: number, value: T['TValue'] | null): any {
        return vector.acceptOperatorVisitor && vector.acceptOperatorVisitor(this, index, value) || null;
    }
    visitMany<T extends DataType>(vectors: Partial<VisitorNode>[], index: number, value: T['TValue'] | null): any[] {
        return vectors.map((vector) => this.visit(vector, index, value));
    }
    abstract visitNull                               (vector: NullVector,             index: number, value: Null['TValue']             | null): any;
    abstract visitBool                               (vector: BoolVector,             index: number, value: Bool['TValue']             | null): any;
    abstract visitInt            <T extends Int>     (vector: IntVector<T>,           index: number, value: Int['TValue']              | null): any;
    abstract visitFloat          <T extends Float>   (vector: FloatVector<T>,         index: number, value: Float['TValue']            | null): any;
    abstract visitUtf8                               (vector: Utf8Vector,             index: number, value: Utf8['TValue']             | null): any;
    abstract visitBinary                             (vector: BinaryVector,           index: number, value: Binary['TValue']           | null): any;
    abstract visitFixedSizeBinary                    (vector: FixedSizeBinaryVector,  index: number, value: FixedSizeBinary['TValue']  | null): any;
    abstract visitDate                               (vector: DateVector,             index: number, value: Date_['TValue']            | null): any;
    abstract visitTimestamp                          (vector: TimestampVector,        index: number, value: Timestamp['TValue']        | null): any;
    abstract visitTime                               (vector: TimeVector,             index: number, value: Time['TValue']             | null): any;
    abstract visitDecimal                            (vector: DecimalVector,          index: number, value: Decimal['TValue']          | null): any;
    abstract visitList           <T extends DataType>(vector: ListVector<T>,          index: number, value: List<T>['TValue']          | null): any;
    abstract visitStruct                             (vector: StructVector,           index: number, value: Struct['TValue']           | null): any;
    abstract visitUnion          <T extends Union>   (vector: UnionVector<T>,         index: number, value: T['TValue']                | null): any;
    abstract visitDictionary     <T extends DataType>(vector: DictionaryVector<T>,    index: number, value: T['TValue']                | null): any;
    abstract visitInterval                           (vector: IntervalVector,         index: number, value: Interval['TValue']         | null): any;
    abstract visitFixedSizeList  <T extends DataType>(vector: FixedSizeListVector<T>, index: number, value: FixedSizeList<T>['TValue'] | null): any;
    abstract visitMap                                (vector: MapVector,              index: number, value: Map_['TValue']             | null): any;

    static visitTypeInline<T extends DataType>(visitor: OperatorVisitor, type: T, vector: Vector<T>, index: number, value: T['TValue'] | null): any {
        switch (type.TType) {
            case Type.Null:            return visitor.visitNull(vector             as any as NullVector,             index, value);
            case Type.Bool:            return visitor.visitBool(vector             as any as BoolVector,             index, value);
            case Type.Int:             return visitor.visitInt(vector              as any as IntVector<T & Int>,     index, value);
            case Type.Float:           return visitor.visitFloat(vector            as any as FloatVector<T & Float>, index, value);
            case Type.Utf8:            return visitor.visitUtf8(vector             as any as Utf8Vector,             index, value);
            case Type.Binary:          return visitor.visitBinary(vector           as any as BinaryVector,           index, value);
            case Type.FixedSizeBinary: return visitor.visitFixedSizeBinary(vector  as any as FixedSizeBinaryVector,  index, value);
            case Type.Date:            return visitor.visitDate(vector             as any as DateVector,             index, value);
            case Type.Timestamp:       return visitor.visitTimestamp(vector        as any as TimestampVector,        index, value);
            case Type.Time:            return visitor.visitTime(vector             as any as TimeVector,             index, value);
            case Type.Decimal:         return visitor.visitDecimal(vector          as any as DecimalVector,          index, value);
            case Type.List:            return visitor.visitList(vector             as any as ListVector<T>,          index, value);
            case Type.Struct:          return visitor.visitStruct(vector           as any as StructVector,           index, value);
            case Type.Union:           return visitor.visitUnion(vector            as any as UnionVector<T & Union>, index, value);
            case Type.Dictionary:      return visitor.visitDictionary(vector       as any as DictionaryVector<T>,    index, value);
            case Type.Interval:        return visitor.visitInterval(vector         as any as IntervalVector,         index, value);
            case Type.FixedSizeList:   return visitor.visitFixedSizeList(vector    as any as FixedSizeListVector<T>, index, value);
            case Type.Map:             return visitor.visitMap(vector              as any as MapVector,              index, value);
            default: return null;
        }
    }
}
