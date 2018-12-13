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

// @ts-check

const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const glob = promisify(require('glob'));
const { zip } = require('ix/iterable/zip');
const child_process = require(`child_process`);
const asyncDone = promisify(require('async-done'));
const argv = require(`command-line-args`)(cliOpts(), { partial: true });

const exists = async (p) => {
    try {
        return !!(await fs.promises.stat(p));
    } catch (e) { return false; }
}

(async () => {

    if (!argv.mode) {
        return print_usage();
    }

    let mode = argv.mode.toUpperCase();
    let jsonPaths = [...(argv.json || [])];
    let filePaths = [...(argv.file || [])];
    let streamPaths = [...(argv.stream || [])];

    if (mode === 'VALIDATE' && !jsonPaths.length) {
        jsonPaths = await glob(path.resolve(__dirname, `../test/data/json/`, `*.json`));
        if (!filePaths.length) {
            [jsonPaths, filePaths] = await loadJSONAndArrowPaths(jsonPaths, 'cpp', 'file');
            [jsonPaths, filePaths] = await loadJSONAndArrowPaths(jsonPaths, 'java', 'file');
        }
        if (!streamPaths.length) {
            [jsonPaths, filePaths] = await loadJSONAndArrowPaths(jsonPaths, 'cpp', 'stream');
            [jsonPaths, filePaths] = await loadJSONAndArrowPaths(jsonPaths, 'java', 'stream');
        }
        for (let [jsonPath, filePath, streamPath] of zip(jsonPaths, filePaths, streamPaths)) {
            console.log(`jsonPath: ${jsonPath}`);
            console.log(`filePath: ${filePath}`);
            console.log(`streamPath: ${streamPath}`);
        }
    }

    if (!jsonPaths.length) {
        return print_usage();
    }

    switch (mode) {
        case 'VALIDATE':

            const args = [`test`, `-i`].concat(argv._unknown || []);
            const gulp = require.resolve(path.join(__dirname, `../node_modules/gulp/bin/gulp.js`));

            for (let [jsonPath, filePath, streamPath] of zip(jsonPaths, filePaths, streamPaths)) {
                args.push('-j', jsonPath, '-f', filePath, '-s', streamPath);
            }

            await asyncDone(() => child_process.spawn(gulp, args, {
                cwd: path.resolve(__dirname, '..'),
                stdio: ['ignore', 'inherit', 'inherit']
            }));

            break;
        default:
            return print_usage();
    }
})()
.then((x) => x || 0, (e) => {
    process.stdout.write(`${e}`);
    return process.exitCode || 1;
}).then((code) => process.exit(code));

async function loadJSONAndArrowPaths(jsonPaths, source, format) {
    const jPaths = [];
    const aPaths = [];
    for (const jsonPath of jsonPaths) {
        const { name } = path.parse(jsonPath);
        const arrowPath = path.resolve(__dirname, `../test/data/${source}/${format}/${name}.arrow`);
        if (await exists(arrowPath)) {
            jPaths.push(jsonPath);
            aPaths.push(arrowPath);
        }
    }
    return [jPaths, aPaths];
}

function cliOpts() {
    return [
        {
            type: String,
            name: 'mode',
            description: 'The integration test to run'
        },
        { name: 'json',   alias: 'j', type: String, multiple: true, defaultValue: [], description: 'The JSON file[s] to read/write' },
        { name: 'file',   alias: 'f', type: String, multiple: true, defaultValue: [], description: 'The Arrow file[s] to read/write' },
        { name: 'stream', alias: 's', type: String, multiple: true, defaultValue: [], description: 'The Arrow stream[s] to read/write' },
    
        {
            type: String,
            name: 'arrow', alias: 'a',
            multiple: true, defaultValue: [],
            description: 'The Arrow file[s] to read/write'
        },
        {
            type: String,
            name: 'json', alias: 'j',
            multiple: true, defaultValue: [],
            description: 'The JSON file[s] to read/write'
        }
    ];
}

function print_usage() {
    console.log(require('command-line-usage')([
        {
            header: 'integration',
            content: 'Script for running Arrow integration tests'
        },
        {
            header: 'Synopsis',
            content: [
                '$ integration.js -j file.json -a file.arrow --mode validate'
            ]
        },
        {
            header: 'Options',
            optionList: [
                ...cliOpts(),
                {
                    name: 'help',
                    description: 'Print this usage guide.'
                }
            ]
        },
    ]));
    return 1;
}
