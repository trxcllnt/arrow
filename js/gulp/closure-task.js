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

const {
    targetDir,
    mainExport,
    esmRequire,
    gCCLanguageNames,
    publicModulePaths,
    observableFromStreams,
    shouldRunInChildProcess,
    spawnGulpCommandInChildProcess,
} = require('./util');

const fs = require('fs');
const gulp = require('gulp');
const path = require('path');
const mkdirp = require('mkdirp');
const { ReplaySubject } = require('rxjs');
const sourcemaps = require('gulp-sourcemaps');
const { memoizeTask } = require('./memoize-task');
const { compileBinFiles } = require('./typescript-task');
const closureCompiler = require('google-closure-compiler').gulp();

const closureTask = ((cache) => memoizeTask(cache, function closure(target, format) {

    if (shouldRunInChildProcess(target, format)) {
        return spawnGulpCommandInChildProcess('build', target, format);
    }

    const src = targetDir(target, `cls`);
    const out = targetDir(target, format);
    const externs = path.join(`${out}/Arrow.externs.js`);
    const entry = path.join(src, `${mainExport}.dom.cls`);
    mkdirp.sync(out);
    fs.writeFileSync(externs, generateClosurePublicExportFile(target, `cls`, false));
    // closure compiler erases static properties/methods
    // https://github.com/google/closure-compiler/issues/1776
    fs.writeFileSync(`${src}/Arrow.dom.cls.js`, generateClosurePublicExportFile(target, `cls`, true));
    return observableFromStreams(
        gulp.src([
/*   external libs first --> */ `node_modules/tslib/package.json`,
                                `node_modules/tslib/tslib.es6.js`,
                                `node_modules/flatbuffers/package.json`,
                                `node_modules/flatbuffers/js/flatbuffers.mjs`,
                                `node_modules/text-encoding-utf-8/package.json`,
                                `node_modules/text-encoding-utf-8/src/encoding.js`,
/*    then sources globs --> */ `${src}/**/*.js`,
        ], { base: `./` }),
        sourcemaps.init(),
        closureCompiler(createClosureArgs(entry, externs)),
        // rename the sourcemaps from *.js.map files to *.min.js.map
        sourcemaps.write(`.`, { mapFile: (mapPath) => mapPath.replace(`.js.map`, `.${target}.min.js.map`) }),
        gulp.dest(out)
    )
    .merge(compileBinFiles(target, format))
    .takeLast(1)
    .publish(new ReplaySubject()).refCount();
}))({});

const createClosureArgs = (entry, externs) => ({
    externs,
    third_party: true,
    warning_level: `QUIET`,
    dependency_mode: `STRICT`,
    rewrite_polyfills: false,
    entry_point: `${entry}.js`,
    module_resolution: `NODE`,
    // formatting: `PRETTY_PRINT`,
    // debug: true,
    compilation_level: `ADVANCED`,
    allow_method_call_decomposing: true,
    package_json_entry_names: `module,jsnext:main,main`,
    assume_function_wrapper: true,
    js_output_file: `${mainExport}.js`,
    language_in: gCCLanguageNames[`es2015`],
    language_out: gCCLanguageNames[`es5`],
    output_wrapper:`${apacheHeader()}
(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
    typeof define === 'function' && define.amd ? define(['exports'], factory) :
    (factory(global.Arrow = global.Arrow || {}));
}(this, (function (exports) {%output%}.bind(this))));`
});

module.exports = closureTask;
module.exports.closureTask = closureTask;

function generateClosurePublicExportFile(target, format, exportSourceForClosure = false) {

    const src = path.resolve('./', targetDir(target, format));
    const exportedImports = publicModulePaths(src).reduce((entries, publicModulePath) => [
        ...entries, {
            publicModulePath,
            exports_: getExportedNames(esmRequire(publicModulePath, { warnings: false }))
        }
    ], []);

    if (exportSourceForClosure) {
        return `${exportedImports.map(({ publicModulePath }, i) => {
            const p = publicModulePath.slice(src.length + 1);
            return (`import * as exports${i} from './${p}';`);
        }).filter(Boolean).join('\n')};\n${'Object.assign(arguments[0], exports0);'}`;
    }

    return `${externsHeader()}\n${exportedImports.map(({ publicModulePath, exports_ }) => {
        const body = exports_.map(externBody).filter(Boolean).join('\n');
        return body ? `// Exported from ${publicModulePath}\n${body}\n` : ``;
    }).filter(Boolean).join('\n')}`;

    function getExportedNames(entryModule) {
        const fn = function() {};
        const isStaticOrProtoName = (x) => (
            !(x in fn) &&
            (x !== `default`) &&
            (x !== `undefined`) &&
            (x !== `__esModule`) &&
            (x !== `constructor`) &&
            !(x.startsWith('_'))
        );
        return Object
            .getOwnPropertyNames(entryModule)
            .filter((name) => (
                typeof entryModule[name] === `object` ||
                typeof entryModule[name] === `function`
            ))
            .map((name) => [name, entryModule[name]])
            .reduce((reserved, [name, value]) => {

                const staticNames = value &&
                    typeof value === 'object' ? Object.getOwnPropertyNames(value).filter(isStaticOrProtoName) :
                    typeof value === 'function' ? Object.getOwnPropertyNames(value).filter(isStaticOrProtoName) : [];

                const instanceNames = (typeof value === `function` && Object.getOwnPropertyNames(value.prototype || {}) || []).filter(isStaticOrProtoName);

                return [...reserved, { exportName: name, staticNames, instanceNames }];
            }, []);
    }

    function externBody({ exportName, staticNames, instanceNames }) {
        return [
            `var ${exportName} = function() {};`,
            staticNames.map((staticName) => (isNaN(+staticName)
                ? `/** @type {?} */\n${exportName}.${staticName} = function() {};`
                : `/** @type {?} */\n${exportName}[${staticName}] = function() {};`
            )).join('\n'),
            instanceNames.map((instanceName) => (isNaN(+instanceName)
                ? `/** @type {?} */\n${exportName}.prototype.${instanceName};`
                : `/** @type {?} */\n${exportName}.prototype[${instanceName}];`
            )).join('\n')
        ].filter(Boolean).join('\n');
    }

    function externsHeader() {
        return (`${apacheHeader()}
// @ts-nocheck
/* tslint:disable */
/**
 * @fileoverview Closure Compiler externs for Arrow
 * @externs
 * @suppress {duplicate,checkTypes}
 */
/** @type {symbol} */
Symbol.iterator;
/** @type {symbol} */
Symbol.asyncIterator;
`);
    }
}

function apacheHeader() {
    return `// Licensed to the Apache Software Foundation (ASF) under one
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
// under the License.`
}