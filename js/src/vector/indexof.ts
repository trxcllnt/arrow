import { OperatorVisitor } from '../visitor';
import { Vector, DictionaryVector } from '../vector';
import { RowView, MapRowView } from './nested';
import { getBool, iterateBits } from '../util/bit';
import { DataType, Dictionary, FlatType, FlatListType } from '../type';
import { Utf8, Binary, Decimal, FixedSizeBinary } from '../type';
import { List, FixedSizeList, Union, Map_, Struct } from '../type';
import { Bool, Null, Int, Float, Date_, Time, Interval, Timestamp } from '../type';

export class IndexOfVisitor extends OperatorVisitor {
    public visitNull           <T extends Null>           (vec: Vector<T>,                   _index: number, value: T['TValue'] | null): number { return value === null && vec.length > 0 ? 0 : -1; } // if you're looking for nulls and the view isn't empty, we've got 'em!
    public visitBool           <T extends Bool>           (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatIndexOf(vector, value); }
    public visitInt            <T extends Int>            (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatIndexOf(vector, value); }
    public visitFloat          <T extends Float>          (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatIndexOf(vector, value); }
    public visitUtf8           <T extends Utf8>           (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatIndexOf(vector, value); }
    public visitBinary         <T extends Binary>         (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatListIndexOf(vector, value); }
    public visitFixedSizeBinary<T extends FixedSizeBinary>(vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatIndexOf(vector, value); }
    public visitDate           <T extends Date_>          (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return dateIndexOf(vector, value); }
    public visitTimestamp      <T extends Timestamp>      (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatIndexOf(vector, value); }
    public visitTime           <T extends Time>           (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatIndexOf(vector, value); }
    public visitDecimal        <T extends Decimal>        (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatListIndexOf(vector, value); }
    public visitList           <T extends DataType>       (vector: Vector<List<T>>,          _index: number, value: T['TValue'] | null): number { return listIndexOf(vector, value); }
    public visitStruct         <T extends Struct>         (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return indexOfVectorLike(vector, value); }
    public visitUnion          <T extends Union<any>>     (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatIndexOf(vector, value); }
    public visitDictionary     <T extends DataType>       (vector: Vector<Dictionary<T>>,    _index: number, value: T['TValue'] | null): number {
        // First find the dictionary key for the desired value...
        const key = this.visit((vector as DictionaryVector<T>).dictionary, _index, value);
        // ... then find the first occurence of that key in indices
        return key === -1 ? -1 : this.visit((vector as DictionaryVector<T>).indices, _index, key);
    }
    public visitInterval       <T extends Interval>       (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return flatIndexOf(vector, value); }
    public visitFixedSizeList  <T extends DataType>       (vector: Vector<FixedSizeList<T>>, _index: number, value: T['TValue'] | null): number { return listIndexOf(vector, value); }
    public visitMap            <T extends Map_>           (vector: Vector<T>,                _index: number, value: T['TValue'] | null): number { return mapIndexOf(vector, value); }
}

function indexOfNull(vector: Vector<any>): number {
    const { nullBitmap } = vector;
    if (!nullBitmap || vector.nullCount <= 0) {
        return -1;
    }
    let i = 0;
    for (const isNull of iterateBits(nullBitmap, vector.data.offset, vector.length, nullBitmap, getBool)) {
        if (isNull) { return i; }
        ++i;
    }
    return -1;
}

function dateIndexOf<T extends Date_>(vector: Vector<T>, search: T['TValue'] | null): number {
    if (search === undefined) { return -1; }
    if (search === null) { return indexOfNull(vector); }
    for (let x = null, i = -1, n = vector.length, v = search.valueOf(); ++i < n;) {
        if ((x = vector.get(i)) && x.valueOf() === v) {
            return i;
        }
    }
    return -1;
}

function flatIndexOf<T extends FlatType | Union<any>>(vector: Vector<T>, search: T['TValue'] | null): number {
    if (search === undefined) { return -1; }
    if (search === null) { return indexOfNull(vector); }
    for (let i = -1, n = vector.length; ++i < n;) {
        if (vector.get(i) === search) {
            return i;
        }
    }
    return -1;
}

function flatListIndexOf<T extends Decimal | FlatListType>(vector: Vector<T>, search: T['TValue'] | null): number {
    if (search === undefined) { return -1; }
    if (search === null) { return indexOfNull(vector); }
    searching:
    for (let x = null, j = 0, i = -1, n = vector.length, k = search.length; ++i < n;) {
        if ((x = vector.get(i)) && (j = x.length) === k) {
            while (--j > -1) {
                if (x[j] !== search[j]) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}

function listIndexOf<T extends DataType>(vector: Vector<List<T> | FixedSizeList<T>>, search: Vector<T> | ArrayLike<T> | null): number {
    return Array.isArray(search) || ArrayBuffer.isView(search)
        ? indexOfArrayLike(vector, search)
        : indexOfVectorLike(vector, search as Vector);
}

function indexOfVectorLike<T extends DataType>(vector: Vector<List<T> | FixedSizeList<T> | Struct>, search: Vector<T> | RowView | null): number {
    if (search === undefined) { return -1; }
    if (search === null) { return indexOfNull(vector); }
    searching:
    for (let x = null, j = 0, i = -1, n = vector.length, k = search.length; ++i < n;) {
        if ((x = vector.get(i)) && (j = x.length) === k) {
            while (--j > -1) {
                if (x.get(j) !== search.get(j)) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}

function indexOfArrayLike<T extends DataType>(vector: Vector<List<T> | FixedSizeList<T>>, search: ArrayLike<T>): number {
    searching:
    for (let x = null, j = 0, i = -1, n = vector.length, k = search.length; ++i < n;) {
        if ((x = vector.get(i)) && (j = x.length) === k) {
            while (--j > -1) {
                if (x.get(j) !== search[j]) {
                    continue searching;
                }
            }
            return i;
        }
    }
    return -1;
}

function mapIndexOf<T extends Map_>(vector: Vector<T>, search: MapRowView | { [k: string]: any } | null): number {
    if (search === undefined) { return -1; }
    if (search === null) { return indexOfNull(vector); }
    if (search instanceof MapRowView) { return indexOfMapView(vector, search); }
    const entries = Object.entries(search);
    searching:
    for (let x = null, i = -1, n = vector.length, k = entries.length; ++i < n;) {
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

function indexOfMapView<T extends Map_>(vector: Vector<T>, search: MapRowView): number {
    searching:
    for (let x = null, i = -1, n = vector.length; ++i < n;) {
        if (x = vector.get(i)) {
            let r1, it1 = x[Symbol.iterator]();
            let r2, it2 = search[Symbol.iterator]();
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
