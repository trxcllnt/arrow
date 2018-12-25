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

import '../jest-extensions';
import * as generate from '../generate-test-data';
import { Table, RecordBatch, Vector, util } from '../Arrow';
const { createElementComparator: compare } = util;

describe('Generated Test Data', () => {
    describe('Table',                      () => validateTable(generate.table([100, 150, 75])));
    describe('RecordBatch',                () => validateRecordBatch(generate.recordBatch()));
    describe('NullVector',                 () => validateVector(generate.null_()));
    describe('BoolVector',                 () => validateVector(generate.bool()));
    describe('Int8Vector',                 () => validateVector(generate.int8()));
    describe('Int16Vector',                () => validateVector(generate.int16()));
    describe('Int32Vector',                () => validateVector(generate.int32()));
    describe('Int64Vector',                () => validateVector(generate.int64()));
    describe('Uint8Vector',                () => validateVector(generate.uint8()));
    describe('Uint16Vector',               () => validateVector(generate.uint16()));
    describe('Uint32Vector',               () => validateVector(generate.uint32()));
    describe('Uint64Vector',               () => validateVector(generate.uint64()));
    describe('Float16Vector',              () => validateVector(generate.float16()));
    describe('Float32Vector',              () => validateVector(generate.float32()));
    describe('Float64Vector',              () => validateVector(generate.float64()));
    describe('Utf8Vector',                 () => validateVector(generate.utf8()));
    describe('BinaryVector',               () => validateVector(generate.binary()));
    describe('FixedSizeBinaryVector',      () => validateVector(generate.fixedSizeBinary()));
    describe('DateDayVector',              () => validateVector(generate.dateDay()));
    describe('DateMillisecondVector',      () => validateVector(generate.dateMillisecond()));
    describe('TimestampSecondVector',      () => validateVector(generate.timestampSecond()));
    describe('TimestampMillisecondVector', () => validateVector(generate.timestampMillisecond()));
    describe('TimestampMicrosecondVector', () => validateVector(generate.timestampMicrosecond()));
    describe('TimestampNanosecondVector',  () => validateVector(generate.timestampNanosecond()));
    describe('TimeSecondVector',           () => validateVector(generate.timeSecond()));
    describe('TimeMillisecondVector',      () => validateVector(generate.timeMillisecond()));
    describe('TimeMicrosecondVector',      () => validateVector(generate.timeMicrosecond()));
    describe('TimeNanosecondVector',       () => validateVector(generate.timeNanosecond()));
    describe('DecimalVector',              () => validateVector(generate.decimal()));
    describe('ListVector',                 () => validateVector(generate.list()));
    describe('StructVector',               () => validateVector(generate.struct()));
    describe('DenseUnionVector',           () => validateVector(generate.denseUnion()));
    describe('SparseUnionVector',          () => validateVector(generate.sparseUnion()));
    describe('DictionaryVector',           () => validateVector(generate.dictionary()));
    describe('IntervalDayTimeVector',      () => validateVector(generate.intervalDayTime()));
    describe('IntervalYearMonthVector',    () => validateVector(generate.intervalYearMonth()));
    describe('FixedSizeListVector',        () => validateVector(generate.fixedSizeList()));
    describe('MapVector',                  () => validateVector(generate.map()));
});

function validateTable({ rows, cols, rowBatches, colBatches, table }: { rows: () => any[][], cols: () => any[][], rowBatches: (() => any[][])[], colBatches: (() => any[][])[], table: Table }) {

    validateVector({ values: rows, vector: table });

    table.chunks.forEach((recordBatch, i) => {
        describe(`recordBatch ${i}`, () => {
            validateRecordBatch({ rows: rowBatches[i], cols: colBatches[i], recordBatch });
        });
    });

    table.schema.fields.forEach((field, i) => {
        describe(`column ${i}: ${field}`, () => validateVector({ values: () => cols()[i], vector: table.getColumnAt(i)! }));
    });
}

function validateRecordBatch({ rows, cols, recordBatch }: { rows: () => any[][], cols: () => any[][], recordBatch: RecordBatch }) {

    validateVector({ values: rows, vector: recordBatch });

    recordBatch.schema.fields.forEach((field, i) => {
        describe(`${field}`, () => validateVector({ values: () => cols()[i], vector: recordBatch.getChildAt(i)! }));
    });
}

function validateVector({ values: createValues, vector }: { values: () => any[], vector: Vector }) {

    const values = createValues();

    test(`gets expected values`, () => {
        expect.hasAssertions();
        let i = -1, n = vector.length, actual, expected;
        try {
            while (++i < n) {
                actual = vector.get(i);
                expected = values[i];
                expect(actual).toArrowCompare(expected);
            }
        } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
    });

    test(`sets expected values`, () => {
        expect.hasAssertions();
        let i = -1, n = vector.length, actual, expected;
        try {
            while (++i < n) {
                expected = vector.get(i);
                vector.set(i, expected);
                actual = vector.get(i);
                expect(actual).toArrowCompare(expected);
            }
        } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
    });

    test(`iterates expected values`, () => {
        expect.hasAssertions();
        let i = -1, actual, expected;
        try {
            for (actual of vector) {
                expected = values[++i];
                expect(actual).toArrowCompare(expected);
            }
        } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
    });

    test(`indexOf returns expected values`, () => {
        expect.hasAssertions();
        let i = -1, n = vector.length;
        const shuffled = shuffle(values);
        let value: any, actual, expected;
        try {
            while (++i < n) {
                value = shuffled[i];
                actual = vector.indexOf(value);
                expected = values.findIndex(compare(value));
                expect(actual).toBe(expected);
            }
            // I would be pretty surprised if randomatic ever generates these values
            expect(vector.indexOf('purple elephants')).toBe(-1);
            expect(vector.indexOf('whistling wombats')).toBe(-1);
            expect(vector.indexOf('carnivorous novices')).toBe(-1);
        } catch (e) { throw new Error(`${vector}[${i}]: ${e}`); }
    });
}

function shuffle(input: any[]) {
    const result = input.slice();
    let j, tmp, i = result.length;
    while (--i > 0) {
        j = (Math.random() * (i + 1)) | 0;
        tmp = result[i];
        result[i] = result[j];
        result[j] = tmp;
    }
    return result;
}
