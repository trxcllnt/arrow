import { VType } from '../vector';
import { VectorVisitor } from '../visitor';
import { Type, DType, DataType } from '../type';
import { TextDecoder } from 'text-encoding-utf-8';

const decodeUtf8 = ((decoder) =>
    decoder.decode.bind(decoder) as (input?: ArrayBufferLike | ArrayBufferView) => string
)(new TextDecoder('utf-8'));

export const epochSecondsToMs = (data: Int32Array, index: number) => 1000 * data[index];
export const epochDaysToMs = (data: Int32Array, index: number) => 86400000 * data[index];
export const epochMillisecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1]) + (data[index] >>> 0);
export const epochMicrosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000) + ((data[index] >>> 0) / 1000);
export const epochNanosecondsLongToMs = (data: Int32Array, index: number) => 4294967296 * (data[index + 1] / 1000000) + ((data[index] >>> 0) / 1000000);

export const epochMillisecondsToDate = (epochMs: number) => new Date(epochMs);
export const epochDaysToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochDaysToMs(data, index));
export const epochSecondsToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochSecondsToMs(data, index));
export const epochNanosecondsLongToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochNanosecondsLongToMs(data, index));
export const epochMillisecondsLongToDate = (data: Int32Array, index: number) => epochMillisecondsToDate(epochMillisecondsLongToMs(data, index));

type TValues = { [T in Type]: DType[T]['TValue'] };

export class GetVisitor extends VectorVisitor<TValues> {
    // @ts-ignore
    visitNull(vector: VType[Type.Null], index: number) { return null; }
    visitBool({ offset, values }: VType[Type.Bool], index: number) {
        const idx = offset + index;
        const byte = values[idx >> 3];
        return (byte & 1 << (idx % 8)) !== 0;
    }
    visitInt8({ stride, values }: VType[Type.Int8], index: number) {
        return values[stride * index];
    }
    visitInt16({ stride, values }: VType[Type.Int16], index: number) {
        return values[stride * index];
    }
    visitInt32({ stride, values }: VType[Type.Int32], index: number) {
        return values[stride * index];
    }
    visitUint8({ stride, values }: VType[Type.Uint8], index: number) {
        return values[stride * index];
    }
    visitUint16({ stride, values }: VType[Type.Uint16], index: number) {
        return values[stride * index];
    }
    visitUint32({ stride, values }: VType[Type.Uint32], index: number) {
        return values[stride * index];
    }
    visitFloat16({ stride, values }: VType[Type.Float16], index: number) {
        return values[stride * index];
    }
    visitFloat32({ stride, values }: VType[Type.Float32], index: number) {
        return values[stride * index];
    }
    visitFloat64({ stride, values }: VType[Type.Float64], index: number) {
        return values[stride * index];
    }
    visitInt64({ values }: VType[Type.Int64], index: number) {
        return values.subarray(2 * index, 2 * index + 1);
    }
    visitUint64({ values }: VType[Type.Uint64], index: number) {
        return values.subarray(2 * index, 2 * index + 1);
    }
    visitUtf8({ values, valueOffsets }: VType[Type.Utf8], index: number) {
        return decodeUtf8(values.subarray(valueOffsets[index], valueOffsets[index + 1]));
    }
    visitBinary({ values, valueOffsets }: VType[Type.Binary], index: number) {
        return values.subarray(valueOffsets[index], valueOffsets[index + 1]);
    }
    visitFixedSizeBinary({ stride, values }: VType[Type.FixedSizeBinary], index: number) {
        return values.subarray(stride * index, stride * (index + 1));
    }
    visitDateDay({ values }: VType[Type.DateDay], index: number) {
        return epochDaysToDate(values, index * 2);
    }
    visitDateMillisecond({ values }: VType[Type.DateMillisecond], index: number) {
        return epochMillisecondsLongToDate(values, index * 2);
    }
    visitTimestampSecond({ values }: VType[Type.TimestampSecond], index: number) {
        return epochSecondsToMs(values, index * 2);
    }
    visitTimestampMillisecond({ values }: VType[Type.TimestampMillisecond], index: number) {
        return epochMillisecondsLongToMs(values, index * 2);
    }
    visitTimestampMicrosecond({ values }: VType[Type.TimestampMicrosecond], index: number) {
        return epochMicrosecondsLongToMs(values, index * 2);
    }
    visitTimestampNanosecond({ values }: VType[Type.TimestampNanosecond], index: number) {
        return epochNanosecondsLongToMs(values, index * 2);
    }
    visitTimeSecond({ values }: VType[Type.TimeSecond], index: number) {
        return values[2 * index];
    }
    visitTimeMillisecond({ values }: VType[Type.TimeMillisecond], index: number) {
        return values[2 * index];
    }
    visitTimeMicrosecond({ values }: VType[Type.TimeMicrosecond], index: number) {
        return values.subarray(2 * index, 2 * index + 1);
    }
    visitTimeNanosecond({ values }: VType[Type.TimeNanosecond], index: number) {
        return values.subarray(2 * index, 2 * index + 1);
    }
    visitDecimal({ values }: VType[Type.Decimal], index: number) {
        return values.subarray(4 * index, 4 * (index + 1));
    }
    visitList<T extends DataType>(vector: VType<T>[Type.List], index: number) {
        const child = vector.getChildAt(0)!, { valueOffsets, stride } = vector;
        return child.slice(valueOffsets[index * stride], valueOffsets[(index * stride) + 1]);
    }
    visitFixedSizeList<T extends DataType>(vector: VType<T>[Type.FixedSizeList], index: number) {
        const child = vector.getChildAt(0)!, { stride } = vector;
        return child.slice(index * stride, (index + 1) * stride);
    }
    visitDenseUnion(vector: VType[Type.DenseUnion], index: number) {
        const { typeIds, type: { typeIdToChildIndex } } = vector;
        const child = vector.getChildAt(typeIdToChildIndex[typeIds[index] as Type]);
        return child ? child.get(vector.valueOffsets[index]) : null;
    }
    visitSparseUnion(vector: VType[Type.SparseUnion], index: number) {
        const { typeIds, type: { typeIdToChildIndex } } = vector;
        const child = vector.getChildAt(typeIdToChildIndex[typeIds[index] as Type]);
        return child ? child.get(index) : null;
    }
    visitDictionary<T extends DataType>(vector: VType<T>[Type.Dictionary], index: number) {
        const key = vector.indices.get(index) as number;
        const val = vector.type.dictionary.get(key);
        return val;
    }
    visitIntervalDayTime({ values }: VType[Type.IntervalDayTime], index: number) {
        return values.subarray(2 * index, 2 * index + 1);
    }
    visitIntervalYearMonth({ values }: VType[Type.IntervalYearMonth], index: number) {
        const interval = values[2 * index];
        const i32s = new Int32Array(2);
        i32s[0] = interval / 12; /* years */
        i32s[1] = interval % 12; /* months */
        return i32s;
    }
    visitStruct(vector: VType[Type.Struct], index: number) {
        const { numChildren } = vector;
        const values = new Array(numChildren);
        for (let i = -1; ++i < numChildren;) {
            values[i] = vector.getChildAt(i)!.get(index);
        }
        return values as any;
    }
    visitMap(vector: VType[Type.Map], index: number) {
        const { numChildren, type: { children }} = vector;
        const values = Object.create(null);
        for (let i = -1; ++i < numChildren;) {
            const field = children[i];
            const child = vector.getChildAt(i)!;
            values[field.name] = child.get(index);
        }
        return values;
    }
}
