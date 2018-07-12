import { OperatorVisitor } from '../visitor';
import { RowView, MapRowView } from '../vector/nested';
import { getBool, iterateBits } from '../util/bit';
import {
    DataType, Dictionary,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Union, Map_, Struct,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp
} from '../type';

import { Vector, DictionaryVector, DateVector } from '../vector';

export class IndexOfVisitor extends OperatorVisitor {
    public visitNull            <T extends Null>            (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return       nullIndexOf(vector, clamp(vector, index), value); }
    public visitBool            <T extends Bool>            (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      valueIndexOf(vector, clamp(vector, index), value); }
    public visitInt             <T extends Int>             (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      valueIndexOf(vector, clamp(vector, index), value); }
    public visitFloat           <T extends Float>           (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      valueIndexOf(vector, clamp(vector, index), value); }
    public visitUtf8            <T extends Utf8>            (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      valueIndexOf(vector, clamp(vector, index), value); }
    public visitBinary          <T extends Binary>          (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      arrayIndexOf(vector, clamp(vector, index), value); }
    public visitFixedSizeBinary <T extends FixedSizeBinary> (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      valueIndexOf(vector, clamp(vector, index), value); }
    public visitDate            <T extends Date_>           (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return       dateIndexOf(vector, clamp(vector, index), value); }
    public visitTimestamp       <T extends Timestamp>       (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      valueIndexOf(vector, clamp(vector, index), value); }
    public visitTime            <T extends Time>            (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      valueIndexOf(vector, clamp(vector, index), value); }
    public visitDecimal         <T extends Decimal>         (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      arrayIndexOf(vector, clamp(vector, index), value); }
    public visitList            <T extends List>            (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return       listIndexOf(vector, clamp(vector, index), value); }
    public visitStruct          <T extends Struct>          (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return indexOfVectorLike(vector, clamp(vector, index), value); }
    public visitUnion           <T extends Union>           (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      valueIndexOf(vector, clamp(vector, index), value); }
    public visitDictionary      <T extends Dictionary>      (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return dictionaryIndexOf(vector, clamp(vector, index), value); }
    public visitInterval        <T extends Interval>        (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return      valueIndexOf(vector, clamp(vector, index), value); }
    public visitFixedSizeList   <T extends FixedSizeList>   (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return       listIndexOf(vector, clamp(vector, index), value); }
    public visitMap             <T extends Map_>            (vector: Vector<T>,                value: T['TValue'] | null, index: number): number { return        mapIndexOf(vector, clamp(vector, index), value); }
}

function clamp<T extends DataType>(vector: Vector<T>, fromIndex: number) {
    return fromIndex < 0 ? (vector.length + (fromIndex % vector.length)) : fromIndex;
}

function nullIndexOf(vector: Vector<Null>, fromIndex: number, searchElement: null) {
     // if you're looking for nulls and the view isn't empty, we've got 'em!
    return searchElement === null && vector.length > 0 ? fromIndex : -1;
}

function indexOfNull<T extends DataType>(vector: Vector<T>, fromIndex: number, ): number {
    const { nullBitmap } = vector;
    if (!nullBitmap || vector.nullCount <= 0) {
        return -1;
    }
    let i = 0;
    for (const isNull of iterateBits(nullBitmap, vector.data.offset + fromIndex, vector.length, nullBitmap, getBool)) {
        if (isNull) { return i; }
        ++i;
    }
    return -1;
}

function dateIndexOf(vector: Vector<Date_>, fromIndex: number, searchElement: Date | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    return valueIndexOf((vector as DateVector).asEpochMilliseconds(), fromIndex, searchElement.valueOf());
}

function dictionaryIndexOf<T extends DataType>(vector: Vector<Dictionary<T>>, fromIndex: number, searchElement: T['TValue']): number {
    const { dictionary, indices } = (vector as DictionaryVector<T>);
    // First find the dictionary key for the desired value...
    const key = dictionary.indexOf(searchElement, fromIndex);
    // ... then find the first occurence of that key in indices
    return key === -1 ? -1 : indices.indexOf(key, fromIndex);
}

function listIndexOf<T extends DataType>(vector: Vector<List<T> | FixedSizeList<T>>, fromIndex: number, searchElement: Vector<T> | ArrayLike<T> | null): number {
    return Array.isArray(searchElement) || ArrayBuffer.isView(searchElement)
        ? indexOfArrayLike(vector, fromIndex, searchElement)
        : indexOfVectorLike(vector, fromIndex, searchElement as Vector);
}

function valueIndexOf<T extends DataType>(vector: Vector<T>, fromIndex: number, searchElement: T['TValue'] | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    for (let i = fromIndex - 1, n = vector.length; ++i < n;) {
        if (vector.get(i) === searchElement) {
            return i;
        }
    }
    return -1;
}

function arrayIndexOf<T extends DataType>(vector: Vector<T>, fromIndex: number, searchElement: T['TValue'] | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    searching:
    for (let x = null, j = 0, i = fromIndex - 1, n = vector.length, k = searchElement.length; ++i < n;) {
        if ((x = vector.get(i)) && (j = x.length) === k) {
            while (--j > -1) {
                if (x[j] !== searchElement[j]) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}

function indexOfVectorLike<T extends DataType>(vector: Vector<List<T> | FixedSizeList<T> | Struct>, fromIndex: number, searchElement: Vector<T> | RowView | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    searching:
    for (let x = null, j = 0, i = fromIndex - 1, n = vector.length, k = searchElement.length; ++i < n;) {
        if ((x = vector.get(i)) && (j = x.length) === k) {
            while (--j > -1) {
                if (x.get(j) !== searchElement.get(j)) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}

function indexOfArrayLike<T extends DataType>(vector: Vector<List<T> | FixedSizeList<T>>, fromIndex: number, searchElement: ArrayLike<T>): number {
    searching:
    for (let x = null, j = 0, i = fromIndex - 1, n = vector.length, k = searchElement.length; ++i < n;) {
        if ((x = vector.get(i)) && (j = x.length) === k) {
            while (--j > -1) {
                if (x.get(j) !== searchElement[j]) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}

function mapIndexOf(vector: Vector<Map_>, fromIndex: number, searchElement: MapRowView | { [k: string]: any } | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    if (searchElement instanceof MapRowView) { return indexOfMapView(vector, fromIndex, searchElement); }
    const entries = Object.entries(searchElement);
    searching:
    for (let x = null, i = fromIndex - 1, n = vector.length, k = entries.length; ++i < n;) {
        if (x = vector.get(i)) {
            for (let j = -1; ++j < k;) {
                let [key, val] = entries[j];
                if (x.get(key as any) !== val) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}

function indexOfMapView(vector: Vector<Map_>, fromIndex: number, searchElement: MapRowView): number {
    searching:
    for (let x = null, i = fromIndex - 1, n = vector.length; ++i < n;) {
        if (x = vector.get(i)) {
            let r1, it1 = x[Symbol.iterator]();
            let r2, it2 = searchElement[Symbol.iterator]();
            while (!(r1 = it1.next()).done && !(r2 = it2.next()).done) {
                if (r1.value !== r2.value) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}
