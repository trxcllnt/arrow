import { Type } from '../type';
import * as vecs from '../vector';
import { TypeVisitor } from '../visitor';

export class TypeToVectorVisitor extends TypeVisitor<vecs.VCtor> {
    public visitNull                 (_: Type.Null                 ) { return vecs.NullVector; }
    public visitBool                 (_: Type.Bool                 ) { return vecs.BoolVector; }
    public visitInt                  (_: Type.Int                  ) { return vecs.IntVector; }
    public visitInt8                 (_: Type.Int8                 ) { return vecs.Int8Vector; }
    public visitInt16                (_: Type.Int16                ) { return vecs.Int16Vector; }
    public visitInt32                (_: Type.Int32                ) { return vecs.Int32Vector; }
    public visitInt64                (_: Type.Int64                ) { return vecs.Int64Vector; }
    public visitUint8                (_: Type.Uint8                ) { return vecs.Uint8Vector; }
    public visitUint16               (_: Type.Uint16               ) { return vecs.Uint16Vector; }
    public visitUint32               (_: Type.Uint32               ) { return vecs.Uint32Vector; }
    public visitUint64               (_: Type.Uint64               ) { return vecs.Uint64Vector; }
    public visitFloat                (_: Type.Float                ) { return vecs.FloatVector; }
    public visitFloat16              (_: Type.Float16              ) { return vecs.Float16Vector; }
    public visitFloat32              (_: Type.Float32              ) { return vecs.Float32Vector; }
    public visitFloat64              (_: Type.Float64              ) { return vecs.Float64Vector; }
    public visitUtf8                 (_: Type.Utf8                 ) { return vecs.Utf8Vector; }
    public visitBinary               (_: Type.Binary               ) { return vecs.BinaryVector; }
    public visitFixedSizeBinary      (_: Type.FixedSizeBinary      ) { return vecs.FixedSizeBinaryVector; }
    public visitDate                 (_: Type.Date                 ) { return vecs.DateVector; }
    public visitDateDay              (_: Type.DateDay              ) { return vecs.DateDayVector; }
    public visitDateMillisecond      (_: Type.DateMillisecond      ) { return vecs.DateMillisecondVector; }
    public visitTimestamp            (_: Type.Timestamp            ) { return vecs.TimestampVector; }
    public visitTimestampSecond      (_: Type.TimestampSecond      ) { return vecs.TimestampSecondVector; }
    public visitTimestampMillisecond (_: Type.TimestampMillisecond ) { return vecs.TimestampMillisecondVector; }
    public visitTimestampMicrosecond (_: Type.TimestampMicrosecond ) { return vecs.TimestampMicrosecondVector; }
    public visitTimestampNanosecond  (_: Type.TimestampNanosecond  ) { return vecs.TimestampNanosecondVector; }
    public visitTime                 (_: Type.Time                 ) { return vecs.TimeVector; }
    public visitTimeSecond           (_: Type.TimeSecond           ) { return vecs.TimeSecondVector; }
    public visitTimeMillisecond      (_: Type.TimeMillisecond      ) { return vecs.TimeMillisecondVector; }
    public visitTimeMicrosecond      (_: Type.TimeMicrosecond      ) { return vecs.TimeMicrosecondVector; }
    public visitTimeNanosecond       (_: Type.TimeNanosecond       ) { return vecs.TimeNanosecondVector; }
    public visitDecimal              (_: Type.Decimal              ) { return vecs.DecimalVector; }
    public visitList                 (_: Type.List                 ) { return vecs.ListVector; }
    public visitStruct               (_: Type.Struct               ) { return vecs.StructVector; }
    public visitUnion                (_: Type.Union                ) { return vecs.UnionVector; }
    public visitDenseUnion           (_: Type.DenseUnion           ) { return vecs.DenseUnionVector; }
    public visitSparseUnion          (_: Type.SparseUnion          ) { return vecs.SparseUnionVector; }
    public visitDictionary           (_: Type.Dictionary           ) { return vecs.DictionaryVector; }
    public visitInterval             (_: Type.Interval             ) { return vecs.IntervalVector; }
    public visitIntervalDayTime      (_: Type.IntervalDayTime      ) { return vecs.IntervalDayTimeVector; }
    public visitIntervalYearMonth    (_: Type.IntervalYearMonth    ) { return vecs.IntervalYearMonthVector; }
    public visitFixedSizeList        (_: Type.FixedSizeList        ) { return vecs.FixedSizeListVector; }
    public visitMap                  (_: Type.Map                  ) { return vecs.MapVector; }
}
