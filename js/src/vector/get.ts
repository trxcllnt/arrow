import { OperatorVisitor } from '../visitor';
import { Vector, DictionaryVector } from '../vector';
import {
    DataType,
    Utf8, Binary, Decimal, FixedSizeBinary,
    List, FixedSizeList, Union, Map_, Struct,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp
} from '../type';

export class GetVisitor extends OperatorVisitor {
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