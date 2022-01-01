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

const exec = require('child_process').exec;

const bundleTask = (bundler, args = "") => (cb) => {
    if (bundler === 'esbuild') {
        exec(`./test/bundle/esbuild/esbuild.js`, (err, stdout, stderr) => {
            console.log(stdout);
            console.log(stderr);
            cb(err);
        });
    } else {
        exec(`${bundler} --config test/bundle/${bundler}/${bundler}.config.js ${args}`, (err, stdout, stderr) => {
            console.log(stdout);
            console.log(stderr);
            cb(err);
        });
    }
}

module.exports = bundleTask;
module.exports.bundleTask = bundleTask;
