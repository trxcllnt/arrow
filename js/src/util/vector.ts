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

type RangeLike = { length: number; stride?: number };
type ClampThen<T extends RangeLike> = (source: T, index: number) => any;
type ClampRangeThen<T extends RangeLike> = (source: T, offset: number, length: number) => any;

export function clampIndex<T extends RangeLike>(source: T, index: number): number;
export function clampIndex<T extends RangeLike, N extends ClampThen<T> = ClampThen<T>>(source: T, index: number, then: N): ReturnType<N>;
export function clampIndex<T extends RangeLike, N extends ClampThen<T> = ClampThen<T>>(source: T, index: number, then?: N) {
    const length = source.length;
    const adjust = index > -1 ? index : (length + (index % length));
    return then ? then(source, adjust) : adjust;
}

export function clampRange<T extends RangeLike>(source: T, begin: number | undefined, end: number | undefined): [number, number];
export function clampRange<T extends RangeLike, N extends ClampRangeThen<T> = ClampRangeThen<T>>(source: T, begin: number | undefined, end: number | undefined, then: N): ReturnType<N>;
export function clampRange<T extends RangeLike, N extends ClampRangeThen<T> = ClampRangeThen<T>>(source: T, begin: number | undefined, end: number | undefined, then?: N) {

    // maybe this fn can just be
    // let wrapAround = (i, N = length) => ((i % N) + N) % N;

    // Adjust args similar to Array.prototype.slice. Normalize begin/end to
    // clamp between 0 and length, and wrap around on negative indices, e.g.
    // slice(-1, 5) or slice(5, -1)
    let { length = 0, stride = 1 } = source;
    let tmp, len = length, max = length * stride;
    let lhs = (typeof begin !== 'number' ? 0 : begin) * stride;
    let rhs = (typeof end !== 'number' ? length : end) * stride;

    (rhs < 0) && (rhs = (len - (rhs * -1)) % length);
    (lhs < 0) && (lhs = (len - (lhs * -1)) % length);
    (rhs < lhs) && (tmp = lhs, lhs = rhs, rhs = tmp);
    len = Math.min(max, !isFinite(len = (rhs - lhs)) || len < 0 ? 0 : len);
    return then ? then(source, lhs, len) : [lhs, len];
}
