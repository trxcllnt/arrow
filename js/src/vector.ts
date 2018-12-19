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

import { Data } from './data';
import { DataType } from './type';

export abstract class Vector<T extends DataType = any> implements Iterable<T['TValue'] | null> {

    // @ts-ignore
    protected _bindDataAccessors(data: Data<T>) {
        if (this.nullCount > 0) {
            this['get'] && (this['get'] = wrapNullable1(this['get']));
        }
    }

    public abstract readonly type: T;
    public abstract readonly data: Data<T>;
    public abstract readonly length: number;
    public abstract readonly stride: number;
    public abstract readonly nullCount: number;
    public abstract readonly numChildren: number;

    public abstract readonly TType: T['TType'];
    public abstract readonly TArray: T['TArray'];
    public abstract readonly TValue: T['TValue'];
    public abstract readonly ArrayType: T['ArrayType'];

    public abstract isValid(index: number): boolean;
    public abstract get(index: number): T['TValue'] | null;
    public abstract set(index: number, value: T['TValue'] | null): void;
    public abstract indexOf(value: T['TValue'] | null, fromIndex?: number): number;

    public abstract toArray(): T['TArray'];
    public abstract [Symbol.iterator](): IterableIterator<T['TValue'] | null>;
    public abstract slice(begin?: number, end?: number): Vector<T>;
    public abstract concat(this: Vector<T>, ...others: Vector<T>[]): Vector<T>;

    public abstract getChildAt<R extends DataType = any>(index: number): Vector<R> | null;
}

function wrapNullable1<T extends DataType, V extends Vector<T>, F extends (i: number) => any>(fn: F): (...args: Parameters<F>) => ReturnType<F> {
    return function(this: V, i: number) { return this.isValid(i) ? fn.call(this, i) : null; };
}
