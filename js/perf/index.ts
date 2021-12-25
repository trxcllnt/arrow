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

// Alternatively, use bundles for performance tests
// import * as Arrow from '../targets/es5/umd.js';
// import * as Arrow from '../targets/es5/cjs.js';
// import * as Arrow from '../targets/es2015/umd.js';
// import * as Arrow from '../targets/es2015/cjs.js';

import * as Arrow from '../src/Arrow.js';

import config, { arrays, typedArrays } from './config.js';
import b from 'benny';
import { CaseResult, Summary } from 'benny/lib/internal/common-types';
import kleur from 'kleur';

const { RecordBatchReader, RecordBatchStreamWriter } = Arrow;


const args = process.argv.slice(2);
const json = args[0] === '--json';

const formatter = new Intl.NumberFormat();
function formatNumber(number: number, precision = 0) {
    const rounded = number > precision * 10 ? Math.round(number) : parseFloat((number).toPrecision(precision));
    return formatter.format(rounded);
}

const results: CaseResult[] = [];

function cycle(result: CaseResult, _summary: Summary) {
    const duration = result.details.median * 1000;
    if (json) {
        (<any>result).suite = _summary.name;
        results.push(result);
    }
    console.log(
        `${kleur.cyan(result.name)} ${formatNumber(result.ops, 3)} ops/s ±${result.margin.toPrecision(2)}%, ${formatNumber(duration, 2)} ms, ${kleur.gray(`${result.samples} samples`)}`,
    );
}

b.suite(
    `Vector`,

    ...Object.entries(typedArrays).map(([name, array]) =>
        b.add(`make vector from ${name}`, () => {
            Arrow.makeVector(array);
        })),

    ...Object.entries(arrays).map(([name, array]) =>
        b.add(`make vector from ${name}`, () => {
            Arrow.vectorFromArray(array as any);
        })),

    b.cycle(cycle)
);

for (const { name, ipc, table } of config) {
    b.suite(
        `Parse`,

        b.add(`dataset: ${name}, function: read recordBatches`, () => {
            for (const _recordBatch of RecordBatchReader.from(ipc)) { }
        }),

        b.add(`dataset: ${name}, function: write recordBatches`, () => {
            RecordBatchStreamWriter.writeAll(table).toUint8Array(true);
        }),

        b.cycle(cycle)
    );

    const schema = table.schema;

    const suites = [{
        suite_name: `Get values by index`,
        fn(vector: Arrow.Vector<any>) {
            for (let i = -1, n = vector.length; ++i < n;) {
                vector.get(i);
            }
        }
    }, {
        suite_name: `Iterate vectors`,
        fn(vector: Arrow.Vector<any>) { for (const _value of vector) { } }
    }, {
        suite_name: `Slice toArray vectors`,
        fn(vector: Arrow.Vector<any>) { vector.slice().toArray(); }
    }, {
        suite_name: `Slice vectors`,
        fn(vector: Arrow.Vector<any>) { vector.slice(); }
    }];

    for (const { suite_name, fn } of suites) {
        b.suite(
            suite_name,

            ...schema.fields.map((f, i) => {
                const vector = table.getChildAt(i)!;
                return b.add(`dataset: ${name}, column: ${f.name}, length: ${formatNumber(vector.length)}, type: ${vector.type}`, () => {
                    fn(vector);
                });
            }),

            b.cycle(cycle)
        );
    }
}


for (const { name, table, counts } of config) {
    b.suite(
        `Table Iterate`,

        b.add(`dataset: ${name}, numRows: ${formatNumber(table.numRows)}`, () => {
            for (const _value of table) { }
        }),

        b.cycle(cycle)
    );

    b.suite(
        `Table Direct Count`,

        ...counts.map(({ column, test, value }: { column: string; test: 'gt' | 'eq'; value: number | string }) => b.add(
            `dataset: ${name}, column: ${column}, numRows: ${formatNumber(table.numRows)}, type: ${table.schema.fields.find((c) => c.name === column)!.type}, test: ${test}, value: ${value}`,
            () => {
                const colidx = table.schema.fields.findIndex((c) => c.name === column);

                if (test == 'gt') {
                    return () => {
                        let sum = 0;
                        const batches = table.batches;
                        const numBatches = batches.length;
                        for (let batchIndex = -1; ++batchIndex < numBatches;) {
                            // load batches
                            const batch = batches[batchIndex];
                            const vector = batch.getChildAt(colidx)!;
                            // yield all indices
                            for (let index = -1, length = batch.numRows; ++index < length;) {
                                sum += (vector.get(index) >= value) ? 1 : 0;
                            }
                        }
                        return sum;
                    };
                } else if (test == 'eq') {
                    return () => {
                        let sum = 0;
                        const batches = table.batches;
                        const numBatches = batches.length;
                        for (let batchIndex = -1; ++batchIndex < numBatches;) {
                            // load batches
                            const batch = batches[batchIndex];
                            const vector = batch.getChildAt(colidx)!;
                            // yield all indices
                            for (let index = -1, length = batch.numRows; ++index < length;) {
                                sum += (vector.get(index) === value) ? 1 : 0;
                            }
                        }
                        return sum;
                    };
                } else {
                    throw new Error(`Unrecognized test "${test}"`);
                }
            }
        )),

        b.cycle(cycle),

        b.complete(() => {
            // last benchmark finished
            json && process.stderr.write(JSON.stringify(results, null, 2));
        })
    );
}
