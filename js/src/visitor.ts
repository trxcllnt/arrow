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

import { Data } from './data';
import { Vector } from './vector';
import {
    Type, DataType, Dictionary,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Union, Map_, Struct,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp
} from './type';

export abstract class Visitor {
    abstract visitNull            <T extends Null>            (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitBool            <T extends Bool>            (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitInt             <T extends Int>             (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitFloat           <T extends Float>           (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitUtf8            <T extends Utf8>            (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitBinary          <T extends Binary>          (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitFixedSizeBinary <T extends FixedSizeBinary> (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitDate            <T extends Date_>           (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitTimestamp       <T extends Timestamp>       (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitTime            <T extends Time>            (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitDecimal         <T extends Decimal>         (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitList            <T extends List>            (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitStruct          <T extends Struct>          (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitUnion           <T extends Union>           (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitDictionary      <T extends Dictionary>      (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitInterval        <T extends Interval>        (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitFixedSizeList   <T extends FixedSizeList>   (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    abstract visitMap             <T extends Map_>            (node?: T | Data<T> | Vector<T>, ...args: any[]): any;
    static bindVisitor<TType extends Type>(visitor: Visitor, typeId: TType): any {
        switch (typeId) {
            case Type.Null:            return visitor.visitNull.bind(visitor);
            case Type.Bool:            return visitor.visitBool.bind(visitor);
            case Type.Int:             return visitor.visitInt.bind(visitor);
            case Type.Float:           return visitor.visitFloat.bind(visitor);
            case Type.Utf8:            return visitor.visitUtf8.bind(visitor);
            case Type.Binary:          return visitor.visitBinary.bind(visitor);
            case Type.FixedSizeBinary: return visitor.visitFixedSizeBinary.bind(visitor);
            case Type.Date:            return visitor.visitDate.bind(visitor);
            case Type.Timestamp:       return visitor.visitTimestamp.bind(visitor);
            case Type.Time:            return visitor.visitTime.bind(visitor);
            case Type.Decimal:         return visitor.visitDecimal.bind(visitor);
            case Type.List:            return visitor.visitList.bind(visitor);
            case Type.Struct:          return visitor.visitStruct.bind(visitor);
            case Type.Union:           return visitor.visitUnion.bind(visitor);
            case Type.Dictionary:      return visitor.visitDictionary.bind(visitor);
            case Type.Interval:        return visitor.visitInterval.bind(visitor);
            case Type.FixedSizeList:   return visitor.visitFixedSizeList.bind(visitor);
            case Type.Map:             return visitor.visitMap.bind(visitor);
            default: return null;
        }
    }
    static visitTypeInline<T extends DataType>(visitor: Visitor, node: T | Data<T> | Vector<T>, ...args: any[]): any {
        switch (node.TType) {
            case Type.Null:            return visitor.visitNull            && visitor.visitNull            <T & Null           >(node as any, ...args);
            case Type.Bool:            return visitor.visitBool            && visitor.visitBool            <T & Bool           >(node as any, ...args);
            case Type.Int:             return visitor.visitInt             && visitor.visitInt             <T & Int            >(node as any, ...args);
            case Type.Float:           return visitor.visitFloat           && visitor.visitFloat           <T & Float          >(node as any, ...args);
            case Type.Utf8:            return visitor.visitUtf8            && visitor.visitUtf8            <T & Utf8           >(node as any, ...args);
            case Type.Binary:          return visitor.visitBinary          && visitor.visitBinary          <T & Binary         >(node as any, ...args);
            case Type.FixedSizeBinary: return visitor.visitFixedSizeBinary && visitor.visitFixedSizeBinary <T & FixedSizeBinary>(node as any, ...args);
            case Type.Date:            return visitor.visitDate            && visitor.visitDate            <T & Date_          >(node as any, ...args);
            case Type.Timestamp:       return visitor.visitTimestamp       && visitor.visitTimestamp       <T & Timestamp      >(node as any, ...args);
            case Type.Time:            return visitor.visitTime            && visitor.visitTime            <T & Time           >(node as any, ...args);
            case Type.Decimal:         return visitor.visitDecimal         && visitor.visitDecimal         <T & Decimal        >(node as any, ...args);
            case Type.List:            return visitor.visitList            && visitor.visitList            <T & List           >(node as any, ...args);
            case Type.Struct:          return visitor.visitStruct          && visitor.visitStruct          <T & Struct         >(node as any, ...args);
            case Type.Union:           return visitor.visitUnion           && visitor.visitUnion           <T & Union          >(node as any, ...args);
            case Type.Dictionary:      return visitor.visitDictionary      && visitor.visitDictionary      <T & Dictionary     >(node as any, ...args);
            case Type.Interval:        return visitor.visitInterval        && visitor.visitInterval        <T & Interval       >(node as any, ...args);
            case Type.FixedSizeList:   return visitor.visitFixedSizeList   && visitor.visitFixedSizeList   <T & FixedSizeList  >(node as any, ...args);
            case Type.Map:             return visitor.visitMap             && visitor.visitMap             <T & Map_           >(node as any, ...args);
            default: return null;
        }
    }
}

export interface TypeNode { acceptTypeVisitor(visitor: TypeVisitor, ...args: any[]): any; }
export abstract class TypeVisitor extends Visitor {
    visit(node: TypeNode, ...args: any[]): any { return node.acceptTypeVisitor(this, ...args) || null; }
    visitMany(nodes: TypeNode[], ...args: any[][]): any[] { return nodes.map((node, i) => this.visit(node, ...(args[i] || []))); }
}

export interface VectorNode { acceptVectorVisitor(visitor: VectorVisitor, ...args: any[]): any; }
export abstract class VectorVisitor extends Visitor {
    visit(node: VectorNode, ...args: any[]): any { return node.acceptVectorVisitor(this, ...args) || null; }
    visitMany(nodes: VectorNode[], ...args: any[][]): any[] { return nodes.map((node, i) => this.visit(node, ...(args[i] || []))); }
}

export interface OperatorNode { acceptOperatorVisitor(visitor: OperatorVisitor, ...args: any[]): any; }
export abstract class OperatorVisitor extends Visitor {
    visit(node: OperatorNode, ...args: any[]): any { return node.acceptOperatorVisitor(this, ...args) || null; }
    visitMany(nodes: OperatorNode[], ...args: any[][]): any[] { return nodes.map((node, i) => this.visit(node, ...(args[i] || []))); }
}
