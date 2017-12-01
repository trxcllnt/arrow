import * as Schema_ from '../format/Schema';
import { ArrowType, ArrowTypeVisitor } from './visitor';
import Type = Schema_.org.apache.arrow.flatbuf.Type;
import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
import TimeUnit = Schema_.org.apache.arrow.flatbuf.TimeUnit;
import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
import UnionMode = Schema_.org.apache.arrow.flatbuf.UnionMode;
import IntervalUnit = Schema_.org.apache.arrow.flatbuf.IntervalUnit;

export type IntBitWidth = 8 | 16 | 32 | 64;
export type TimeBitWidth = IntBitWidth | 128;

export abstract class FieldType extends ArrowType {
    constructor(public type: Type) {
        super();
    }
}

export class NullFieldType extends FieldType {
    static create() {
        return new NullFieldType();
    }
    constructor() {
        super(Type.Null);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitNullFieldType(this);
    }
}

export class IntFieldType extends FieldType {
    static create(isSigned: boolean, bitWidth: IntBitWidth) {
        return new IntFieldType(isSigned, bitWidth);
    }
    constructor(public isSigned: boolean, public bitWidth: IntBitWidth) {
        super(Type.Int);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitIntFieldType(this);
    }
}

export class FloatingPointFieldType extends FieldType {
    static create(precision: Precision) {
        return new FloatingPointFieldType(precision);
    }
    constructor(public precision: Precision) {
        super(Type.FloatingPoint);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitFloatingPointFieldType(this);
    }
}

export class BinaryFieldType extends FieldType {
    static create() {
        return new BinaryFieldType();
    }
    constructor() {
        super(Type.Binary);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitBinaryFieldType(this);
    }
}

export class BoolFieldType extends FieldType {
    static create() {
        return new BoolFieldType();
    }
    constructor() {
        super(Type.Bool);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitBoolFieldType(this);
    }
}

export class Utf8FieldType extends FieldType {
    static create() {
        return new Utf8FieldType();
    }
    constructor() {
        super(Type.Utf8);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitUtf8FieldType(this);
    }
}

export class DecimalFieldType extends FieldType {
    static create(scale: number, precision: number) {
        return new DecimalFieldType(scale, precision);
    }
    constructor(public scale: number, public precision: number) {
        super(Type.Decimal);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitDecimalFieldType(this);
    }
}

export class DateFieldType extends FieldType {
    static create(unit: DateUnit) {
        return new DateFieldType(unit);
    }
    constructor(public unit: DateUnit) {
        super(Type.Date);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitDateFieldType(this);
    }
}

export class TimeFieldType extends FieldType {
    static create(unit: TimeUnit, bitWidth: TimeBitWidth) {
        return new TimeFieldType(unit, bitWidth);
    }
    constructor(public unit: TimeUnit, public bitWidth: TimeBitWidth) {
        super(Type.Time);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitTimeFieldType(this);
    }
}

export class TimestampFieldType extends FieldType {
    static create(unit: TimeUnit, timezone?: string) {
        return new TimestampFieldType(unit, timezone);
    }
    constructor(public unit: TimeUnit, public timezone?: string) {
        super(Type.Time);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitTimestampFieldType(this);
    }
}

export class IntervalFieldType extends FieldType {
    static create(unit: IntervalUnit) {
        return new IntervalFieldType(unit);
    }
    constructor(public unit: IntervalUnit) {
        super(Type.Interval);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitIntervalFieldType(this);
    }
}

export class ListFieldType extends FieldType {
    static create() {
        return new ListFieldType();
    }
    constructor() {
        super(Type.Time);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitListFieldType(this);
    }
}

export class StructFieldType extends FieldType {
    static create() {
        return new StructFieldType();
    }
    constructor() {
        super(Type.Time);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitStructFieldType(this);
    }
}

export class UnionFieldType extends FieldType {
    static create(mode: UnionMode, typeIds: Type[]) {
        return new UnionFieldType(mode, typeIds);
    }
    constructor(public mode: UnionMode, public typeIds: Type[]) {
        super(Type.Time);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitUnionFieldType(this);
    }
}

export class FixedSizeBinaryFieldType extends FieldType {
    static create(byteWidth: number) {
        return new FixedSizeBinaryFieldType(byteWidth);
    }
    constructor(public byteWidth: number) {
        super(Type.FixedSizeBinary);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitFixedSizeBinaryFieldType(this);
    }
}

export class FixedSizeListFieldType extends FieldType {
    static create(listSize: number) {
        return new FixedSizeListFieldType(listSize);
    }
    constructor(public listSize: number) {
        super(Type.FixedSizeList);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitFixedSizeListFieldType(this);
    }
}

export class MapFieldType extends FieldType {
    static create(keysSorted: boolean) {
        return new MapFieldType(keysSorted);
    }
    constructor(public keysSorted: boolean) {
        super(Type.Time);
    }
    accept(visitor: ArrowTypeVisitor) {
        return visitor.visitMapFieldType(this);
    }
}

declare module './visitor' {
    namespace ArrowType {
        export let none: typeof NullFieldType.create;
        export let null_: typeof NullFieldType.create;
        export let int: typeof IntFieldType.create;
        export let floatingPoint: typeof FloatingPointFieldType.create;
        export let binary: typeof BinaryFieldType.create;
        export let bool: typeof BoolFieldType.create;
        export let utf8: typeof Utf8FieldType.create;
        export let decimal: typeof DecimalFieldType.create;
        export let date: typeof DateFieldType.create;
        export let time: typeof TimeFieldType.create;
        export let timestamp: typeof TimestampFieldType.create;
        export let interval: typeof IntervalFieldType.create;
        export let list: typeof ListFieldType.create;
        export let struct: typeof StructFieldType.create;
        export let union: typeof UnionFieldType.create;
        export let fixedSizeBinary: typeof FixedSizeBinaryFieldType.create;
        export let fixedSizeList: typeof FixedSizeListFieldType.create;
        export let map: typeof MapFieldType.create;
    }
    interface ArrowTypeVisitor {
        visitNullFieldType(node: NullFieldType): any;
        visitIntFieldType(node: IntFieldType): any;
        visitFloatingPointFieldType(node: FloatingPointFieldType): any;
        visitBinaryFieldType(node: BinaryFieldType): any;
        visitBoolFieldType(node: BoolFieldType): any;
        visitUtf8FieldType(node: Utf8FieldType): any;
        visitDecimalFieldType(node: DecimalFieldType): any;
        visitDateFieldType(node: DateFieldType): any;
        visitTimeFieldType(node: TimeFieldType): any;
        visitTimestampFieldType(node: TimestampFieldType): any;
        visitIntervalFieldType(node: IntervalFieldType): any;
        visitListFieldType(node: ListFieldType): any;
        visitStructFieldType(node: StructFieldType): any;
        visitUnionFieldType(node: UnionFieldType): any;
        visitFixedSizeBinaryFieldType(node: FixedSizeBinaryFieldType): any;
        visitFixedSizeListFieldType(node: FixedSizeListFieldType): any;
        visitMapFieldType(node: MapFieldType): any;
    }
}

ArrowType.none = NullFieldType.create;
ArrowType.null_ = NullFieldType.create;
ArrowType.int = IntFieldType.create;
ArrowType.floatingPoint = FloatingPointFieldType.create;
ArrowType.binary = BinaryFieldType.create;
ArrowType.bool = BoolFieldType.create;
ArrowType.utf8 = Utf8FieldType.create;
ArrowType.decimal = DecimalFieldType.create;
ArrowType.date = DateFieldType.create;
ArrowType.time = TimeFieldType.create;
ArrowType.timestamp = TimestampFieldType.create;
ArrowType.interval = IntervalFieldType.create;
ArrowType.list = ListFieldType.create;
ArrowType.struct = StructFieldType.create;
ArrowType.union = UnionFieldType.create;
ArrowType.fixedSizeBinary = FixedSizeBinaryFieldType.create;
ArrowType.fixedSizeList = FixedSizeListFieldType.create;
ArrowType.map = MapFieldType.create;
