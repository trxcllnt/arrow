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

const fs = require('fs');
const glob = require('glob');
const path = require('path');

const argv = require(`command-line-args`)([
    { name: `all`, type: Boolean },
    { name: 'update', alias: 'u', type: Boolean },
    { name: 'verbose', alias: 'v', type: Boolean },
    { name: `target`, type: String, defaultValue: `` },
    { name: `module`, type: String, defaultValue: `` },
    { name: `coverage`, type: Boolean, defaultValue: false },
    { name: `integration`, alias: `i`, type: Boolean, defaultValue: false },
    { name: `targets`, alias: `t`, type: String, multiple: true, defaultValue: [] },
    { name: `modules`, alias: `m`, type: String, multiple: true, defaultValue: [] },
    { name: `json_files`, alias: `j`, type: String, multiple: true, defaultValue: [] },
    { name: `arrow_files`, alias: `f`, type: String, multiple: true, defaultValue: [] },
    { name: `arrow_streams`, alias: `s`, type: String, multiple: true, defaultValue: [] },
], { partial: true });

const { targets, modules } = argv;

if (argv.target === `src`) {
    argv.target && !targets.length && targets.push(argv.target);
} else {
    argv.target && !targets.length && targets.push(argv.target);
    argv.module && !modules.length && modules.push(argv.module);
    (argv.all || !targets.length) && targets.push(`all`);
    (argv.all || !modules.length) && modules.push(`all`);
}

if (argv.coverage && (!argv.json_files || !argv.json_files.length)) {

    let filePaths = [];
    let streamPaths = [];
    let jsonPaths = glob.sync(path.resolve(__dirname, `../test/data/json/`, `*.json`));

    [jsonPaths, filePaths] = loadJSONAndArrowPaths(jsonPaths, 'cpp', 'file');
    [jsonPaths, filePaths] = loadJSONAndArrowPaths(jsonPaths, 'java', 'file');
    [jsonPaths, streamPaths] = loadJSONAndArrowPaths(jsonPaths, 'cpp', 'stream');
    [jsonPaths, streamPaths] = loadJSONAndArrowPaths(jsonPaths, 'java', 'stream');

    argv.json_files = jsonPaths;
    argv.arrow_files = filePaths;
    argv.arrow_streams = streamPaths;
}

module.exports = { argv, targets, modules };

function loadJSONAndArrowPaths(jsonPaths, source, format) {
    const jPaths = [];
    const aPaths = [];
    for (const jsonPath of jsonPaths) {
        const { name } = path.parse(jsonPath);
        const arrowPath = path.resolve(__dirname, `../test/data/${source}/${format}/${name}.arrow`);
        if (fs.existsSync(arrowPath)) {
            jPaths.push(jsonPath);
            aPaths.push(arrowPath);
        }
    }
    return [jPaths, aPaths];
}
