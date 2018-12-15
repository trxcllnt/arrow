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
    ESKeywords,
    UMDSourceTargets,
    terserLanguageNames
} = require('./util');

const path = require('path');
const webpack = require(`webpack`);
const { memoizeTask } = require('./memoize-task');
const { compileBinFiles } = require('./typescript-task');
const { Observable, ReplaySubject } = require('rxjs');
const TerserPlugin = require(`terser-webpack-plugin`);
const esmRequire = require(`esm`)(module, {
    mode: `auto`,
    cjs: {
        /* A boolean for storing ES modules in require.cache. */
        cache: true,
        /* A boolean for respecting require.extensions in ESM. */
        extensions: true,
        /* A boolean for __esModule interoperability. */
        interop: true,
        /* A boolean for importing named exports of CJS modules. */
        namedExports: true,
        /* A boolean for following CJS path rules in ESM. */
        paths: true,
        /* A boolean for __dirname, __filename, and require in ESM. */
        vars: true,
    }
});

const minifyTask = ((cache, commonConfig) => memoizeTask(cache, function minifyJS(target, format) {

    const sourceTarget = UMDSourceTargets[target];
    const PublicNames = reservePublicNames(sourceTarget, `cls`);
    const out = targetDir(target, format), src = targetDir(sourceTarget, `cls`);

    const targetConfig = { ...commonConfig,
        output: { ...commonConfig.output,
            path: path.resolve(`./${out}`) } };

    const webpackConfigs = [
        [`${mainExport}.dom`, PublicNames]
    ].map(([entry, reserved]) => ({
        ...targetConfig,
        name: entry,
        entry: { [entry]: path.resolve(`${src}/${entry}.js`) },
        plugins: [
            ...(targetConfig.plugins || []),
            new webpack.SourceMapDevToolPlugin({
                filename: `[name].${target}.min.js.map`,
                moduleFilenameTemplate: ({ resourcePath }) =>
                    resourcePath
                        .replace(/\s/, `_`)
                        .replace(/\.\/node_modules\//, ``)
            }),
            new TerserPlugin({
                sourceMap: true,
                terserOptions: {
                    ecma: terserLanguageNames[target],
                    compress: { unsafe: true },
                    output: { comments: false, beautify: false },
                    mangle: { eval: true,
                        properties: { reserved, keep_quoted: true }
                    },
                    safari10: true // <-- works around safari10 bugs, see the "safari10" option here: https://github.com/terser-js/terser#minify-options
                },
            })
        ]
    }));

    const compilers = webpack(webpackConfigs);
    return Observable
            .bindNodeCallback(compilers.run.bind(compilers))()
            .merge(compileBinFiles(target, format)).takeLast(1)
            .multicast(new ReplaySubject()).refCount();
}))({}, {
    resolve: { mainFields: [`module`, `main`] },
    module: { rules: [{ test: /\.js$/, enforce: `pre`, use: [`source-map-loader`] }] },
    output: { filename: '[name].js', library: mainExport, libraryTarget: `umd`, umdNamedDefine: true },
});

module.exports = minifyTask;
module.exports.minifyTask = minifyTask;

const reservePublicNames = ((ESKeywords) => function reservePublicNames(target, format) {
    const src = targetDir(target, format);
    const publicModulePaths = [
        `../${src}/data.js`,
        `../${src}/type.js`,
        `../${src}/table.js`,
        `../${src}/column.js`,
        `../${src}/schema.js`,
        `../${src}/vector.js`,
        `../${src}/visitor.js`,
        `../${src}/util/int.js`,
        `../${src}/recordbatch.js`,
        `../${src}/compute/dataframe.js`,
        `../${src}/compute/predicate.js`,
        `../${src}/${mainExport}.dom.js`,
    ];
    return publicModulePaths.reduce((keywords, publicModulePath) => [
        ...keywords, ...reserveExportedNames(esmRequire(publicModulePath, { warnings: false }))
    ], [...ESKeywords]);
})(ESKeywords);

// Reflect on the Arrow modules to come up with a list of keys to save from Terser's
// mangler. Assume all the non-inherited static and prototype members of the Arrow
// module and its direct exports are public, and should be preserved through minification.
const reserveExportedNames = (entryModule) => (
    Object
        .getOwnPropertyNames(entryModule)
        .filter((name) => (
            typeof entryModule[name] === `object` ||
            typeof entryModule[name] === `function`
        ))
        .map((name) => [name, entryModule[name]])
        .reduce((reserved, [name, value]) => {
            const fn = function() {};
            const ownKeys = value && typeof value === 'object' && Object.getOwnPropertyNames(value) || [];
            const protoKeys = typeof value === `function` && Object.getOwnPropertyNames(value.prototype || {}) || [];
            const publicNames = [...ownKeys, ...protoKeys].filter((x) => x !== `default` && x !== `undefined` && !(x in fn));
            return [...reserved, name, ...publicNames];
        }, []
    )
);
