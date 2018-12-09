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

export * from './enum';
export * from './data';
export * from './type';
export * from './table';
export * from './column';
export * from './schema';
export * from './vector';
export * from './ipc/reader';
export * from './ipc/message';
export * from './recordbatch';
export * from './io/interfaces';

// import * as enums from './enum';
// import * as type_ from './type';
// import * as vector_ from './vector';
// import * as visitor_ from './visitor';
// import * as util_int_ from './util/int';
// import * as util_bit_ from './util/bit';
// import * as util_node from './util/node';
// // import * as predicate_ from './predicate';

// import { Data } from './data';
// import { Type } from './enum';
// import { Vector } from './vector';
// import { Schema, Field } from './schema';
// import { RecordBatch } from './recordbatch';
// import { Table, DataFrame } from './table';
// // import { Table, DataFrame, NextFunc, BindFunc, CountByResult } from './table';

// export import IntBitWidth = type_.IntBitWidth;
// export import TimeBitWidth = type_.TimeBitWidth;

// export { Table, DataFrame };
// // export { Table, DataFrame, NextFunc, BindFunc, CountByResult };
// export { Field, Schema, RecordBatch, Vector, Type, Data };

// export namespace util {
//     export import Int64 = util_int_.Int64;
//     export import Int128 = util_int_.Int128;
//     export import Uint64 = util_int_.Uint64;
//     export import packBools = util_bit_.packBools;
//     export import PipeIterator = util_node.PipeIterator;
//     export import AsyncPipeIterator = util_node.AsyncPipeIterator;
// }

// export namespace enum_ {
//     export import Type = enums.ArrowType;
//     export import DateUnit = enums.DateUnit;
//     export import TimeUnit = enums.TimeUnit;
//     export import Precision = enums.Precision;
//     export import UnionMode = enums.UnionMode;
//     export import VectorType = enums.VectorType;
//     export import IntervalUnit = enums.IntervalUnit;
//     export import MessageHeader = enums.MessageHeader;
//     export import MetadataVersion = enums.MetadataVersion;
// }

// export namespace type {
//     export import Null = type_.Null;
//     export import Bool = type_.Bool;
//     export import Int = type_.Int;
//     export import Int8 = type_.Int8;
//     export import Int16 = type_.Int16;
//     export import Int32 = type_.Int32;
//     export import Int64 = type_.Int64;
//     export import Uint8 = type_.Uint8;
//     export import Uint16 = type_.Uint16;
//     export import Uint32 = type_.Uint32;
//     export import Uint64 = type_.Uint64;
//     export import Float = type_.Float;
//     export import Float16 = type_.Float16;
//     export import Float32 = type_.Float32;
//     export import Float64 = type_.Float64;
//     export import Utf8 = type_.Utf8;
//     export import Binary = type_.Binary;
//     export import FixedSizeBinary = type_.FixedSizeBinary;
//     export import Date_ = type_.Date_;
//     export import DateDay = type_.DateDay;
//     export import DateMillisecond = type_.DateMillisecond;
//     export import Timestamp = type_.Timestamp;
//     export import TimestampSecond = type_.TimestampSecond;
//     export import TimestampMillisecond = type_.TimestampMillisecond;
//     export import TimestampMicrosecond = type_.TimestampMicrosecond;
//     export import TimestampNanosecond = type_.TimestampNanosecond;
//     export import Time = type_.Time;
//     export import TimeSecond = type_.TimeSecond;
//     export import TimeMillisecond = type_.TimeMillisecond;
//     export import TimeMicrosecond = type_.TimeMicrosecond;
//     export import TimeNanosecond = type_.TimeNanosecond;
//     export import Decimal = type_.Decimal;
//     export import List = type_.List;
//     export import Struct = type_.Struct;
//     export import Union = type_.Union;
//     export import DenseUnion = type_.DenseUnion;
//     export import SparseUnion = type_.SparseUnion;
//     export import Dictionary = type_.Dictionary;
//     export import Interval = type_.Interval;
//     export import IntervalDayTime = type_.IntervalDayTime;
//     export import IntervalYearMonth = type_.IntervalYearMonth;
//     export import FixedSizeList = type_.FixedSizeList;
//     export import Map_ = type_.Map_;
// }

// export namespace vector {
//     export import Vector = vector_.Vector;
//     export import NullVector = vector_.NullVector;
//     export import BoolVector = vector_.BoolVector;
//     export import IntVector = vector_.IntVector;
//     export import Int8Vector = vector_.Int8Vector;
//     export import Int16Vector = vector_.Int16Vector;
//     export import Int32Vector = vector_.Int32Vector;
//     export import Int64Vector = vector_.Int64Vector;
//     export import Uint8Vector = vector_.Uint8Vector;
//     export import Uint16Vector = vector_.Uint16Vector;
//     export import Uint32Vector = vector_.Uint32Vector;
//     export import Uint64Vector = vector_.Uint64Vector;
//     export import FloatVector = vector_.FloatVector;
//     export import Float16Vector = vector_.Float16Vector;

//     export import Float32Vector = vector_.Float32Vector;
//     export import Float64Vector = vector_.Float64Vector;
//     export import DateVector = vector_.DateVector;
//     export import DateDayVector = vector_.DateDayVector;
//     export import DateMillisecondVector = vector_.DateMillisecondVector;
//     export import DecimalVector = vector_.DecimalVector;
//     export import TimestampVector = vector_.TimestampVector;
//     export import TimestampSecondVector = vector_.TimestampSecondVector;
//     export import TimestampMillisecondVector = vector_.TimestampMillisecondVector;
//     export import TimestampMicrosecondVector = vector_.TimestampMicrosecondVector;
//     export import TimestampNanosecondVector = vector_.TimestampNanosecondVector;
//     export import TimeVector = vector_.TimeVector;
//     export import TimeSecondVector = vector_.TimeSecondVector;
//     export import TimeMillisecondVector = vector_.TimeMillisecondVector;
//     export import TimeMicrosecondVector = vector_.TimeMicrosecondVector;
//     export import TimeNanosecondVector = vector_.TimeNanosecondVector;
//     export import IntervalVector = vector_.IntervalVector;
//     export import IntervalDayTimeVector = vector_.IntervalDayTimeVector;
//     export import IntervalYearMonthVector = vector_.IntervalYearMonthVector;
//     export import BinaryVector = vector_.BinaryVector;
//     export import FixedSizeBinaryVector = vector_.FixedSizeBinaryVector;
//     export import Utf8Vector = vector_.Utf8Vector;
//     export import ListVector = vector_.ListVector;
//     export import FixedSizeListVector = vector_.FixedSizeListVector;
//     export import MapVector = vector_.MapVector;
//     export import StructVector = vector_.StructVector;
//     export import UnionVector = vector_.UnionVector;
//     export import DenseUnionVector = vector_.DenseUnionVector;
//     export import SparseUnionVector = vector_.SparseUnionVector;
//     export import DictionaryVector = vector_.DictionaryVector;
// }

// export namespace visitor {
//     export import Visitor = visitor_.Visitor;
// }

// // export namespace predicate {
// //     export import col = predicate_.col;
// //     export import lit = predicate_.lit;
// //     export import and = predicate_.and;
// //     export import or = predicate_.or;
// //     export import custom = predicate_.custom;
// //     export import Or = predicate_.Or;
// //     export import Col = predicate_.Col;
// //     export import And = predicate_.And;
// //     export import Not = predicate_.Not;
// //     export import GTeq = predicate_.GTeq;
// //     export import LTeq = predicate_.LTeq;
// //     export import Value = predicate_.Value;
// //     export import Equals = predicate_.Equals;
// //     export import Literal = predicate_.Literal;
// //     export import Predicate = predicate_.Predicate;

// //     export import PredicateFunc = predicate_.PredicateFunc;
// // }

// /* These exports are needed for the closure and uglify umd targets */
// try {
//     let Arrow: any = eval('exports');
//     if (Arrow && typeof Arrow === 'object') {
//         // string indexers tell closure and uglify not to rename these properties
//         Arrow['type'] = type;
//         Arrow['util'] = util;
//         Arrow['enum_'] = enum_;
//         Arrow['vector'] = vector;
//         Arrow['visitor'] = visitor;
//         // Arrow['predicate'] = predicate;

//         Arrow['Type'] = Type;
//         Arrow['Field'] = Field;
//         Arrow['Schema'] = Schema;
//         Arrow['Vector'] = Vector;
//         Arrow['RecordBatch'] = RecordBatch;

//         // Arrow['Table'] = Table;
//         // Arrow['CountByResult'] = CountByResult;
//     }
// } catch (e) { /* not the UMD bundle */ }
// /* end umd exports */

// // closure compiler erases static properties/methods:
// // https://github.com/google/closure-compiler/issues/1776
// // set them via string indexers to save them from the mangler
// Vector['new'] = Vector.new;
// Schema['from'] = Schema.from;
// // Table['from'] = Table.from;
// // Table['fromAsync'] = Table.fromAsync;
// // Table['fromStruct'] = Table.fromStruct;
// // Table['empty'] = Table.empty;
// // RecordBatch['from'] = RecordBatch.from;

// Data['Null'] = Data.Null;
// Data['Int'] = Data.Int;
// Data['Float'] = Data.Float;
// Data['Bool'] = Data.Bool;
// Data['Decimal'] = Data.Decimal;
// Data['Date'] = Data.Date;
// Data['Time'] = Data.Time;
// Data['Timestamp'] = Data.Timestamp;
// Data['Interval'] = Data.Interval;
// Data['FixedSizeBinary'] = Data.FixedSizeBinary;
// Data['Binary'] = Data.Binary;
// Data['Utf8'] = Data.Utf8;
// Data['List'] = Data.List;
// Data['FixedSizeList'] = Data.FixedSizeList;
// Data['Struct'] = Data.Struct;
// Data['Map'] = Data.Map;
// Data['Union'] = Data.Union;

// util_int_.Uint64['add'] = util_int_.Uint64.add;
// util_int_.Uint64['multiply'] = util_int_.Uint64.multiply;
// util_int_.Uint64['from'] = util_int_.Uint64.from;
// util_int_.Uint64['fromNumber'] = util_int_.Uint64.fromNumber;
// util_int_.Uint64['fromString'] = util_int_.Uint64.fromString;
// util_int_.Uint64['convertArray'] = util_int_.Uint64.convertArray;

// util_int_.Int64['add'] = util_int_.Int64.add;
// util_int_.Int64['multiply'] = util_int_.Int64.multiply;
// util_int_.Int64['from'] = util_int_.Int64.from;
// util_int_.Int64['fromNumber'] = util_int_.Int64.fromNumber;
// util_int_.Int64['fromString'] = util_int_.Int64.fromString;
// util_int_.Int64['convertArray'] = util_int_.Int64.convertArray;

// util_int_.Int128['add'] = util_int_.Int128.add;
// util_int_.Int128['multiply'] = util_int_.Int128.multiply;
// util_int_.Int128['from'] = util_int_.Int128.from;
// util_int_.Int128['fromNumber'] = util_int_.Int128.fromNumber;
// util_int_.Int128['fromString'] = util_int_.Int128.fromString;
// util_int_.Int128['convertArray'] = util_int_.Int128.convertArray;

// type_.DataType['isNull'] = type_.DataType.isNull;
// type_.DataType['isInt'] = type_.DataType.isInt;
// type_.DataType['isFloat'] = type_.DataType.isFloat;
// type_.DataType['isBinary'] = type_.DataType.isBinary;
// type_.DataType['isUtf8'] = type_.DataType.isUtf8;
// type_.DataType['isBool'] = type_.DataType.isBool;
// type_.DataType['isDecimal'] = type_.DataType.isDecimal;
// type_.DataType['isDate'] = type_.DataType.isDate;
// type_.DataType['isTime'] = type_.DataType.isTime;
// type_.DataType['isTimestamp'] = type_.DataType.isTimestamp;
// type_.DataType['isInterval'] = type_.DataType.isInterval;
// type_.DataType['isList'] = type_.DataType.isList;
// type_.DataType['isStruct'] = type_.DataType.isStruct;
// type_.DataType['isUnion'] = type_.DataType.isUnion;
// type_.DataType['isFixedSizeBinary'] = type_.DataType.isFixedSizeBinary;
// type_.DataType['isFixedSizeList'] = type_.DataType.isFixedSizeList;
// type_.DataType['isMap'] = type_.DataType.isMap;
// type_.DataType['isDictionary'] = type_.DataType.isDictionary;

// // vector_.BoolVector['from'] = vector_.BoolVector.from;
// // vector_.DateVector['from'] = vector_.DateVector.from;
// // vector_.IntVector['from'] = vector_.IntVector.from;
// // vector_.FloatVector['from'] = vector_.FloatVector.from;

// // visitor_.TypeVisitor['visitTypeInline'] = visitor_.TypeVisitor.visitTypeInline;
// // visitor_.VectorVisitor['visitTypeInline'] = visitor_.VectorVisitor.visitTypeInline;

// (enums.Type as any)['NONE']            = (enums.ArrowType as any)['NONE']            = enums.Type.NONE;
// (enums.Type as any)['Null']            = (enums.ArrowType as any)['Null']            = enums.Type.Null;
// (enums.Type as any)['Int']             = (enums.ArrowType as any)['Int']             = enums.Type.Int;
// (enums.Type as any)['Float']           = (enums.ArrowType as any)['Float']           = enums.Type.Float;
// (enums.Type as any)['Binary']          = (enums.ArrowType as any)['Binary']          = enums.Type.Binary;
// (enums.Type as any)['Utf8']            = (enums.ArrowType as any)['Utf8']            = enums.Type.Utf8;
// (enums.Type as any)['Bool']            = (enums.ArrowType as any)['Bool']            = enums.Type.Bool;
// (enums.Type as any)['Decimal']         = (enums.ArrowType as any)['Decimal']         = enums.Type.Decimal;
// (enums.Type as any)['Date']            = (enums.ArrowType as any)['Date']            = enums.Type.Date;
// (enums.Type as any)['Time']            = (enums.ArrowType as any)['Time']            = enums.Type.Time;
// (enums.Type as any)['Timestamp']       = (enums.ArrowType as any)['Timestamp']       = enums.Type.Timestamp;
// (enums.Type as any)['Interval']        = (enums.ArrowType as any)['Interval']        = enums.Type.Interval;
// (enums.Type as any)['List']            = (enums.ArrowType as any)['List']            = enums.Type.List;
// (enums.Type as any)['Struct']          = (enums.ArrowType as any)['Struct']          = enums.Type.Struct;
// (enums.Type as any)['Union']           = (enums.ArrowType as any)['Union']           = enums.Type.Union;
// (enums.Type as any)['FixedSizeBinary'] = (enums.ArrowType as any)['FixedSizeBinary'] = enums.Type.FixedSizeBinary;
// (enums.Type as any)['FixedSizeList']   = (enums.ArrowType as any)['FixedSizeList']   = enums.Type.FixedSizeList;
// (enums.Type as any)['Map']             = (enums.ArrowType as any)['Map']             = enums.Type.Map;

// (enums.Type as any)['Dictionary'] = enums.Type.Dictionary;
// (enums.Type as any)['Int8'] = enums.Type.Int8;
// (enums.Type as any)['Int16'] = enums.Type.Int16;
// (enums.Type as any)['Int32'] = enums.Type.Int32;
// (enums.Type as any)['Int64'] = enums.Type.Int64;
// (enums.Type as any)['Uint8'] = enums.Type.Uint8;
// (enums.Type as any)['Uint16'] = enums.Type.Uint16;
// (enums.Type as any)['Uint32'] = enums.Type.Uint32;
// (enums.Type as any)['Uint64'] = enums.Type.Uint64;
// (enums.Type as any)['Float16'] = enums.Type.Float16;
// (enums.Type as any)['Float32'] = enums.Type.Float32;
// (enums.Type as any)['Float64'] = enums.Type.Float64;
// (enums.Type as any)['DateDay'] = enums.Type.DateDay;
// (enums.Type as any)['DateMillisecond'] = enums.Type.DateMillisecond;
// (enums.Type as any)['TimestampSecond'] = enums.Type.TimestampSecond;
// (enums.Type as any)['TimestampMillisecond'] = enums.Type.TimestampMillisecond;
// (enums.Type as any)['TimestampMicrosecond'] = enums.Type.TimestampMicrosecond;
// (enums.Type as any)['TimestampNanosecond'] = enums.Type.TimestampNanosecond;
// (enums.Type as any)['TimeSecond'] = enums.Type.TimeSecond;
// (enums.Type as any)['TimeMillisecond'] = enums.Type.TimeMillisecond;
// (enums.Type as any)['TimeMicrosecond'] = enums.Type.TimeMicrosecond;
// (enums.Type as any)['TimeNanosecond'] = enums.Type.TimeNanosecond;
// (enums.Type as any)['DenseUnion'] = enums.Type.DenseUnion;
// (enums.Type as any)['SparseUnion'] = enums.Type.SparseUnion;
// (enums.Type as any)['IntervalDayTime'] = enums.Type.IntervalDayTime;
// (enums.Type as any)['IntervalYearMonth'] = enums.Type.IntervalYearMonth;
