#! /usr/bin/env node

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

import * as fs from 'fs';
import * as stream from 'stream';
import { promisify } from 'util';
import { valueToString } from '../util/pretty';
import { RecordBatch, RecordBatchReader } from '../Arrow.node';

const padLeft = require('pad-left');
const eos = promisify(stream.finished);
const { parse } = require('json-bignum');
const argv = require(`command-line-args`)(cliOpts(), { partial: true });
const files = argv.help ? [] : [...(argv.file || []), ...(argv._unknown || [])].filter(Boolean);

(async () => {

    const state = { ...argv, hasRecords: false };
    const sources = argv.help ? [] : [
        ...files.map((file) => () => fs.createReadStream(file)),
        () => process.stdin
    ].filter(Boolean) as (() => NodeJS.ReadableStream)[];

    for (const source of sources) {
        const stream = await createRecordBatchStream(source);
        if (stream) {
            await eos(stream
                .pipe(transformRecordBatchRowsToString(state))
                .pipe(process.stdout, { end: false }));
        }
    }

    return state.hasRecords ? 0 : print_usage();
})()
.then((x) => +x || 0, (err) => {
    if (err) {
        console.error(`${err && err.stack || err}`);
    }
    return process.exitCode || 1;
}).then((code) => process.exit(code));

async function createRecordBatchStream(createSourceStream: () => NodeJS.ReadableStream) {

    let source = createSourceStream();
    let reader: RecordBatchReader | null = null;

    try {
        reader = await (await RecordBatchReader.from(source)).open(true);
    } catch (e) { reader = null; }

    if ((!reader || reader.closed) && source instanceof fs.ReadStream) {
        reader = null;
        source.close();
        try {
            let path = source.path;
            let json = parse(await fs.promises.readFile(path, 'utf8'));
            reader = await (await RecordBatchReader.from(json)).open();
        } catch (e) { reader = null; }
    }

    return (reader && !reader.closed) ? reader.toReadableNodeStream() : null;
}

function transformRecordBatchRowsToString(state: { schema: any, separator: string, hasRecords: boolean }) {
    let rowId = 0, separator = `${state.separator || ' |'} `;
    return new stream.Transform({
        encoding: 'utf8',
        writableObjectMode: true,
        readableObjectMode: false,
        transform(batch: RecordBatch, _enc: string, cb: (error?: Error, data?: any) => void) {

            state.hasRecords = state.hasRecords || batch.length > 0;
            if (state.schema && state.schema.length) {
                batch = batch.select(...state.schema);
            }

            const maxColWidths = [11];
            const header = ['row_id', ...batch.schema.fields.map((f) => `${f}`)].map(valueToString);

            header.forEach((x, i) => {
                maxColWidths[i] = Math.max(maxColWidths[i] || 0, x.length);
            });

            // Pass one to convert to strings and count max column widths
            for (let i = -1, n = batch.length - 1; ++i < n;) {
                let row = [rowId + i, ...batch.get(i)!];
                for (let j = -1, k = row.length; ++j < k; ) {
                    maxColWidths[j] = Math.max(maxColWidths[j] || 0, valueToString(row[j]).length);
                }
            }

            for (let i = -1, n = batch.length; ++i < n;) {
                if ((rowId + i) % 350 === 0) {
                    this.push(header
                        .map((x, j) => padLeft(x, maxColWidths[j]))
                        .join(separator) + '\n');
                }
                this.push([rowId + i, ...batch.get(i)!]
                    .map((x) => valueToString(x))
                    .map((x, j) => padLeft(x, maxColWidths[j]))
                    .join(separator) + '\n');
            }
            cb();
        }
    });
}

function cliOpts() {
    return [
        {
            type: String,
            name: 'schema', alias: 's',
            optional: true, multiple: true,
            typeLabel: '{underline columns}',
            description: 'A space-delimited list of column names'
        },
        {
            type: String,
            name: 'file', alias: 'f',
            optional: true, multiple: true,
            description: 'The Arrow file to read'
        },
        {
            type: String,
            name: 'sep', optional: true, default: '|',
            description: 'The column separator character'
        },
        {
            type: Boolean,
            name: 'help', optional: true, default: false,
            description: 'Print this usage guide.'
        }
    ];    
}

function print_usage() {
    console.log(require('command-line-usage')([
        {
            header: 'arrow2csv',
            content: 'Print a CSV from an Arrow file'
        },
        {
            header: 'Synopsis',
            content: [
                '$ arrow2csv {underline file.arrow} [{bold --schema} column_name ...]',
                '$ arrow2csv [{bold --schema} column_name ...] [{bold --file} {underline file.arrow}]',
                '$ arrow2csv {bold -s} column_1 {bold -s} column_2 [{bold -f} {underline file.arrow}]',
                '$ arrow2csv [{bold --help}]'
            ]
        },
        {
            header: 'Options',
            optionList: cliOpts()
        },
        {
            header: 'Example',
            content: [
                '$ arrow2csv --schema foo baz -f simple.arrow --sep ","',
                '> "row_id", "foo: Int32", "bar: Float64", "baz: Utf8"',
                '>        0,            1,              1,        "aa"',
                '>        1,         null,           null,        null',
                '>        2,            3,           null,        null',
                '>        3,            4,              4,       "bbb"',
                '>        4,            5,              5,      "cccc"',
            ]
        }
    ]));
    return 1;
}
