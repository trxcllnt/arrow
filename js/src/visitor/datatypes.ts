import { Type } from '../type';
import * as type from '../type';
import { TypeVisitor } from '../visitor';

export class TypeToDataTypeVisitor extends TypeVisitor<type.DCtor> {
    public visitNull                 (_: Type.Null                 ) { return type.Null; }
    public visitBool                 (_: Type.Bool                 ) { return type.Bool; }
    public visitInt                  (_: Type.Int                  ) { return type.Int; }
    public visitInt8                 (_: Type.Int8                 ) { return type.Int8; }
    public visitInt16                (_: Type.Int16                ) { return type.Int16; }
    public visitInt32                (_: Type.Int32                ) { return type.Int32; }
    public visitInt64                (_: Type.Int64                ) { return type.Int64; }
    public visitUint8                (_: Type.Uint8                ) { return type.Uint8; }
    public visitUint16               (_: Type.Uint16               ) { return type.Uint16; }
    public visitUint32               (_: Type.Uint32               ) { return type.Uint32; }
    public visitUint64               (_: Type.Uint64               ) { return type.Uint64; }
    public visitFloat                (_: Type.Float                ) { return type.Float; }
    public visitFloat16              (_: Type.Float16              ) { return type.Float16; }
    public visitFloat32              (_: Type.Float32              ) { return type.Float32; }
    public visitFloat64              (_: Type.Float64              ) { return type.Float64; }
    public visitUtf8                 (_: Type.Utf8                 ) { return type.Utf8; }
    public visitBinary               (_: Type.Binary               ) { return type.Binary; }
    public visitFixedSizeBinary      (_: Type.FixedSizeBinary      ) { return type.FixedSizeBinary; }
    public visitDate                 (_: Type.Date                 ) { return type.Date_; }
    public visitDateDay              (_: Type.DateDay              ) { return type.DateDay; }
    public visitDateMillisecond      (_: Type.DateMillisecond      ) { return type.DateMillisecond; }
    public visitTimestamp            (_: Type.Timestamp            ) { return type.Timestamp; }
    public visitTimestampSecond      (_: Type.TimestampSecond      ) { return type.TimestampSecond; }
    public visitTimestampMillisecond (_: Type.TimestampMillisecond ) { return type.TimestampMillisecond; }
    public visitTimestampMicrosecond (_: Type.TimestampMicrosecond ) { return type.TimestampMicrosecond; }
    public visitTimestampNanosecond  (_: Type.TimestampNanosecond  ) { return type.TimestampNanosecond; }
    public visitTime                 (_: Type.Time                 ) { return type.Time; }
    public visitTimeSecond           (_: Type.TimeSecond           ) { return type.TimeSecond; }
    public visitTimeMillisecond      (_: Type.TimeMillisecond      ) { return type.TimeMillisecond; }
    public visitTimeMicrosecond      (_: Type.TimeMicrosecond      ) { return type.TimeMicrosecond; }
    public visitTimeNanosecond       (_: Type.TimeNanosecond       ) { return type.TimeNanosecond; }
    public visitDecimal              (_: Type.Decimal              ) { return type.Decimal; }
    public visitList                 (_: Type.List                 ) { return type.List; }
    public visitStruct               (_: Type.Struct               ) { return type.Struct; }
    public visitUnion                (_: Type.Union                ) { return type.Union; }
    public visitDenseUnion           (_: Type.DenseUnion           ) { return type.DenseUnion; }
    public visitSparseUnion          (_: Type.SparseUnion          ) { return type.SparseUnion; }
    public visitDictionary           (_: Type.Dictionary           ) { return type.Dictionary; }
    public visitInterval             (_: Type.Interval             ) { return type.Interval; }
    public visitIntervalDayTime      (_: Type.IntervalDayTime      ) { return type.IntervalDayTime; }
    public visitIntervalYearMonth    (_: Type.IntervalYearMonth    ) { return type.IntervalYearMonth; }
    public visitFixedSizeList        (_: Type.FixedSizeList        ) { return type.FixedSizeList; }
    public visitMap                  (_: Type.Map                  ) { return type.Map_; }
}
