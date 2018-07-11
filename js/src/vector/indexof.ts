import { OperatorVisitor } from '../visitor';
import { RowView, MapRowView } from './nested';
import { getBool, iterateBits } from '../util/bit';
import {
    DataType,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Union, Map_, Struct,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp
} from '../type';

import {
    Vector, DictionaryVector,
    NullVector, BoolVector, IntVector, FloatVector, DecimalVector,
    Utf8Vector, BinaryVector, FixedSizeBinaryVector,
    DateVector, TimestampVector, TimeVector, IntervalVector,
    ListVector, FixedSizeListVector, StructVector, UnionVector, MapVector
} from '../vector';

export class IndexOfVisitor extends OperatorVisitor {
    public visitNull                               (vector: NullVector,             index: number, value: Null['TValue']             | null): number { return nullIndexOf(vector, clamp(vector, index), value); }
    public visitBool                               (vector: BoolVector,             index: number, value: Bool['TValue']             | null): number { return indexOfScalar(vector, clamp(vector, index), value); }
    public visitInt            <T extends Int>     (vector: IntVector<T>,           index: number, value: Int['TValue']              | null): number { return indexOfScalar(vector, clamp(vector, index), value); }
    public visitFloat          <T extends Float>   (vector: FloatVector<T>,         index: number, value: Float['TValue']            | null): number { return indexOfScalar(vector, clamp(vector, index), value); }
    public visitUtf8                               (vector: Utf8Vector,             index: number, value: Utf8['TValue']             | null): number { return indexOfScalar(vector, clamp(vector, index), value); }
    public visitBinary                             (vector: BinaryVector,           index: number, value: Binary['TValue']           | null): number { return indexOfArray(vector, clamp(vector, index), value); }
    public visitFixedSizeBinary                    (vector: FixedSizeBinaryVector,  index: number, value: FixedSizeBinary['TValue']  | null): number { return indexOfScalar(vector, clamp(vector, index), value); }
    public visitDate                               (vector: DateVector,             index: number, value: Date_['TValue']            | null): number { return dateIndexOf(vector, clamp(vector, index), value); }
    public visitTimestamp                          (vector: TimestampVector,        index: number, value: Timestamp['TValue']        | null): number { return indexOfScalar(vector, clamp(vector, index), value); }
    public visitTime                               (vector: TimeVector,             index: number, value: Time['TValue']             | null): number { return indexOfScalar(vector, clamp(vector, index), value); }
    public visitDecimal                            (vector: DecimalVector,          index: number, value: Decimal['TValue']          | null): number { return indexOfArray(vector, clamp(vector, index), value); }
    public visitList           <T extends DataType>(vector: ListVector<T>,          index: number, value: List<T>['TValue']          | null): number { return listIndexOf(vector, clamp(vector, index), value); }
    public visitStruct                             (vector: StructVector,           index: number, value: Struct['TValue']           | null): number { return indexOfVectorLike(vector, clamp(vector, index), value); }
    public visitUnion          <T extends Union>   (vector: UnionVector<T>,         index: number, value: T['TValue']                | null): number { return indexOfScalar(vector, clamp(vector, index), value); }
    public visitDictionary     <T extends DataType>(vector: DictionaryVector<T>,    index: number, value: T['TValue']                | null): number { return dictionaryIndexOf(vector, clamp(vector, index), value); }
    public visitInterval                           (vector: IntervalVector,         index: number, value: Interval['TValue']         | null): number { return indexOfScalar(vector, clamp(vector, index), value); }
    public visitFixedSizeList  <T extends DataType>(vector: FixedSizeListVector<T>, index: number, value: FixedSizeList<T>['TValue'] | null): number { return listIndexOf(vector, clamp(vector, index), value); }
    public visitMap                                (vector: MapVector,              index: number, value: Map_['TValue']             | null): number { return mapIndexOf(vector, clamp(vector, index), value); }
}

function clamp(vector: Vector, fromIndex: number) {
    return fromIndex < 0 ? (vector.length + (fromIndex % vector.length)) : fromIndex;
}

function nullIndexOf(vector: NullVector, fromIndex: number, searchElement: null) {
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

function dateIndexOf(vector: DateVector, fromIndex: number, searchElement: Date | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    return indexOfScalar(vector.asEpochMilliseconds(), fromIndex, searchElement.valueOf());
}

function dictionaryIndexOf<T extends DataType>(vector: DictionaryVector<T>, fromIndex: number, searchElement: T['TValue']): number {
    // First find the dictionary key for the desired value...
    const key = vector.dictionary.indexOf(searchElement, fromIndex);
    // ... then find the first occurence of that key in indices
    return key === -1 ? -1 : vector.indices.indexOf(key, fromIndex);
}

function listIndexOf<T extends DataType>(vector: Vector<List<T> | FixedSizeList<T>>, fromIndex: number, searchElement: Vector<T> | ArrayLike<T> | null): number {
    return Array.isArray(searchElement) || ArrayBuffer.isView(searchElement)
        ? indexOfArrayLike(vector, fromIndex, searchElement)
        : indexOfVectorLike(vector, fromIndex, searchElement as Vector);
}

function indexOfScalar<T extends DataType>(vector: Vector<T>, fromIndex: number, searchElement: T['TValue'] | null): number {
    if (searchElement === undefined) { return -1; }
    if (searchElement === null) { return indexOfNull(vector, fromIndex); }
    for (let i = fromIndex - 1, n = vector.length; ++i < n;) {
        if (vector.get(i) === searchElement) {
            return i;
        }
    }
    return -1;
}

function indexOfArray<T extends DataType>(vector: Vector<T>, fromIndex: number, searchElement: T['TValue'] | null): number {
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

function mapIndexOf(vector: MapVector, fromIndex: number, searchElement: MapRowView | { [k: string]: any } | null): number {
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

function indexOfMapView(vector: MapVector, fromIndex: number, searchElement: MapRowView): number {
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
