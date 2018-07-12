import { Vector } from '../vector';
import { OperatorVisitor } from '../visitor';
import {
    Utf8, Binary, Decimal, FixedSizeBinary,
    Dictionary, List, FixedSizeList, Union, Map_, Struct,
    Bool, Null, Int, Float, Date_, Time, Interval, Timestamp
} from '../type';

export class GetVisitor extends OperatorVisitor {
    public visitNull            <T extends Null>            (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return index < vector.length ? null : undefined; } // if you're looking for nulls and the view isn't empty, we've got 'em!
    public visitBool            <T extends Bool>            (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitInt             <T extends Int>             (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitFloat           <T extends Float>           (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitUtf8            <T extends Utf8>            (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitBinary          <T extends Binary>          (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitFixedSizeBinary <T extends FixedSizeBinary> (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitDate            <T extends Date_>           (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitTimestamp       <T extends Timestamp>       (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitTime            <T extends Time>            (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitDecimal         <T extends Decimal>         (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitList            <T extends List>            (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitStruct          <T extends Struct>          (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitUnion           <T extends Union>           (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitDictionary      <T extends Dictionary>      (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitInterval        <T extends Interval>        (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitFixedSizeList   <T extends FixedSizeList>   (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
    public visitMap             <T extends Map_>            (vector: Vector<T>,                index: number): T['TValue'] | null | undefined { return undefined; }
}
