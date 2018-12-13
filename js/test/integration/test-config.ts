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
import * as Path from 'path';
import { zip } from 'ix/iterable/zip';

/* tslint:disable */
const { parse: bignumJSONParse } = require('json-bignum');

type TestFile = { path: string, data: Uint8Array };

let JSON_FILES = parsePaths(process.env.JSON_FILES);
let ARROW_FILES = parsePaths(process.env.ARROW_FILES);
let ARROW_STREAMS = parsePaths(process.env.ARROW_STREAMS);

if (!JSON_FILES.length && process.env.TEST_FILE_NAMES) {
    JSON_FILES = process.env.TEST_FILE_NAMES.split(' ').map(
        (name) => Path.resolve(__dirname, `../data/json/${name}.json`));
}

if (JSON_FILES.length && !ARROW_FILES.length) {
    [JSON_FILES, ARROW_FILES] = loadDefaultArrowPaths(JSON_FILES, 'cpp', 'file');
    [JSON_FILES, ARROW_FILES] = loadDefaultArrowPaths(JSON_FILES, 'java', 'file');
}

if (JSON_FILES.length && !ARROW_STREAMS.length) {
    [JSON_FILES, ARROW_STREAMS] = loadDefaultArrowPaths(JSON_FILES, 'cpp', 'stream');
    [JSON_FILES, ARROW_STREAMS] = loadDefaultArrowPaths(JSON_FILES, 'java', 'stream');
}

export const jsonAndArrowPaths = [...zip(
    loadJSONAndArrowPaths(JSON_FILES, (b) => bignumJSONParse(`${b}`)),
    loadJSONAndArrowPaths(ARROW_FILES, (b) => b),
    loadJSONAndArrowPaths(ARROW_STREAMS, (b) => b),
)].filter(([p1, p2, p3]) => p1 && p2 && p3) as [TestFile, TestFile, TestFile][];

function parsePaths(input?: string): string[] {
    const paths = JSON.parse(input || '[]');
    return Array.isArray(paths) ? paths : [paths];
}

function loadJSONAndArrowPaths(paths: string | string[], mapData: (x: Buffer) => any) {
    return (Array.isArray(paths) ? paths : [paths]).map((p) => {
        const path = Path.resolve(`${p}`);
        if (fs.existsSync(path)) {
            return { path, data: mapData(fs.readFileSync(path)) };
        }
        console.error(`Could not find file "${path}"`);
        return undefined;
    });
}

function loadDefaultArrowPaths(jsonPaths: string[], source: 'cpp' | 'java', format: 'file' | 'stream') {
    const jPaths = [], aPaths = [];
    for (const jsonPath of jsonPaths) {
        const { name } = Path.parse(jsonPath);
        const arrowPath = Path.resolve(__dirname, `../data/${source}/${format}/${name}.arrow`);
        if (fs.existsSync(arrowPath)) {
            jPaths.push(jsonPath);
            aPaths.push(arrowPath);
        }
    }
    return [jPaths, aPaths];
}
