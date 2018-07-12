import { Vector } from '../vector';
import { VectorVisitor } from '../visitor';
import {
    Utf8, Binary, Decimal, FixedSizeBinary,
    Dictionary, List, FixedSizeList, Union, Map_, Struct,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp
} from '../type';

export class IteratorVisitor extends VectorVisitor {
    public visitNull            <T extends Null>            (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitBool            <T extends Bool>            (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitInt             <T extends Int>             (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitFloat           <T extends Float>           (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitUtf8            <T extends Utf8>            (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitBinary          <T extends Binary>          (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitFixedSizeBinary <T extends FixedSizeBinary> (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitDate            <T extends Date_>           (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitTimestamp       <T extends Timestamp>       (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitTime            <T extends Time>            (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitDecimal         <T extends Decimal>         (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitList            <T extends List>            (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitStruct          <T extends Struct>          (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitUnion           <T extends Union>           (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitDictionary      <T extends Dictionary>      (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitInterval        <T extends Interval>        (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitFixedSizeList   <T extends FixedSizeList>   (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
    public visitMap             <T extends Map_>            (vector: Vector<T>): IterableIterator<T['TValue'] | null> {}
}
