export * from './all';

import { Data } from '../data';
import { Type } from '../enum';
import { Vector } from '../vector';
import { DataType } from '../type';
import { BaseVector } from './base';
import { setBool } from '../util/bit';
import { Vector as V, VectorCtorArgs } from '../interfaces';
import { instance as getVisitor } from '../visitor/get';
import { instance as setVisitor } from '../visitor/set';
import { instance as indexOfVisitor } from '../visitor/indexof';
import { instance as toArrayVisitor } from '../visitor/toarray';
import { instance as iteratorVisitor } from '../visitor/iterator';
import { instance as byteWidthVisitor } from '../visitor/bytewidth';
import { instance as getVectorConstructor } from '../visitor/vectorctor';

declare module '../vector' {
    namespace Vector {
        export { newVector as new };
    }
}

declare module './base' {
    interface BaseVector<T extends DataType> {
        get(index: number): T['TValue'] | null;
        set(index: number, value: T['TValue'] | null): void;
        indexOf(value: T['TValue'] | null, fromIndex?: number): number;
        toArray(): T['TArray'];
        getByteWidth(): number;
        [Symbol.iterator](): IterableIterator<T['TValue'] | null>;
    }
}

Vector.new = newVector;

function newVector<T extends DataType>(data: Data<T>, ...args: VectorCtorArgs<V<T>>): V<T> {
    return new (getVectorConstructor.getVisitFn(data.type)())(data, ...args) as V<T>;
}

//
// We provide the following method implementations for code navigability purposes only.
// They're overridden at runtime below with the specific Visitor implementation for each type,
// short-circuiting the usual Visitor traversal and reducing intermediate lookups and calls.
// This comment is here to remind you to not set breakpoints in these function bodies, or to inform
// you why the breakpoints you have already set are not being triggered. Have a great day!
//

BaseVector.prototype.get = function baseVectorGet<T extends DataType>(this: BaseVector<T>, index: number): T['TValue'] | null {
    return getVisitor.visit(this, index);
};

BaseVector.prototype.set = function baseVectorSet<T extends DataType>(this: BaseVector<T>, index: number, value: T['TValue'] | null): void {
    return setVisitor.visit(this, index, value);
};

BaseVector.prototype.indexOf = function baseVectorIndexOf<T extends DataType>(this: BaseVector<T>, value: T['TValue'] | null, fromIndex?: number): number {
    return indexOfVisitor.visit(this, value, fromIndex);
};

BaseVector.prototype.toArray = function baseVectorToArray<T extends DataType>(this: BaseVector<T>, ): T['TArray'] {
    return toArrayVisitor.visit(this);
};

BaseVector.prototype.getByteWidth = function baseVectorGetByteWidth<T extends DataType>(this: BaseVector<T>, ): number {
    return byteWidthVisitor.visit(this);
};

BaseVector.prototype[Symbol.iterator] = function baseVectorSymbolIterator<T extends DataType>(this: BaseVector<T>, ): IterableIterator<T['TValue'] | null> {
    return iteratorVisitor.visit(this);
};

// Perf: bind and assign the operator Visitor methods to each of the Vector subclasses for each Type
(Object.keys(Type) as any[])
    .filter((TType) => TType !== Type.NONE && TType !== Type[Type.NONE])
    .filter((T: any): T is Type => typeof Type[T] === 'number')
    .forEach((TType) => {
        let typeIds: Type[];
        switch (TType) {
            case Type.Int:       typeIds = [Type.Int8, Type.Int16, Type.Int32, Type.Int64, Type.Uint8, Type.Uint16, Type.Uint32, Type.Uint64]; break;
            case Type.Float:     typeIds = [Type.Float16, Type.Float32, Type.Float64]; break;
            case Type.Date:      typeIds = [Type.DateDay, Type.DateMillisecond]; break;
            case Type.Time:      typeIds = [Type.TimeSecond, Type.TimeMillisecond, Type.TimeMicrosecond, Type.TimeNanosecond]; break;
            case Type.Timestamp: typeIds = [Type.TimestampSecond, Type.TimestampMillisecond, Type.TimestampMicrosecond, Type.TimestampNanosecond]; break;
            case Type.Interval:  typeIds = [Type.IntervalDayTime, Type.IntervalYearMonth]; break;
            case Type.Union:     typeIds = [Type.DenseUnion, Type.SparseUnion]; break;
            default:             typeIds = [TType]; break;
        }
        typeIds.forEach((TType) => {
            const VectorCtor = getVectorConstructor.visit(TType);
            VectorCtor.prototype['get'] = partial1(getVisitor.getVisitFn(TType));
            VectorCtor.prototype['set'] = partial2(setVisitor.getVisitFn(TType));
            VectorCtor.prototype['indexOf'] = partial2(indexOfVisitor.getVisitFn(TType));
            VectorCtor.prototype['toArray'] = partial0(toArrayVisitor.getVisitFn(TType));
            VectorCtor.prototype['getByteWidth'] = partial0(byteWidthVisitor.getVisitFn(TType));
            VectorCtor.prototype[Symbol.iterator] = partial0(iteratorVisitor.getVisitFn(TType));
        });
    });

function partial0<T>(visit: (node: T) => any) {
    return function(this: T) { return visit(this); };
}

function partial1<T>(visit: (node: T, a: any) => any) {
    return function(this: T, a: any) { return visit(this, a); };
}

function partial2<T>(visit: (node: T, a: any, b: any) => any) {
    return function(this: T, a: any, b: any) { return visit(this, a, b); };
}

function wrapNullableSet<T extends DataType, V extends BaseVector<T>, F extends (i: number, a: any) => void>(fn: F): (...args: Parameters<F>) => void {
    return function(this: V, i: number, a: any) {
        if (setBool(this.nullBitmap, this.offset + i, a != null)) {
            fn.call(this, i, a);
        }
    };
}

// @ts-ignore
function bindBaseVectorDataAccessors<T extends DataType>(this: BaseVector<T>, data: Data<T>) {
    const type = this.type;
    this['get'] = getVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
    this['set'] = setVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
    this['indexOf'] = indexOfVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
    this['toArray'] = toArrayVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
    this[Symbol.iterator] = iteratorVisitor.getVisitFn(type).bind(this, <any> this as V<T>);
    if (this.nullCount > 0) {
        this['set'] = wrapNullableSet(this['set']);
    }
    (Vector.prototype as any).bindDataAccessors.call(this, data);
}
