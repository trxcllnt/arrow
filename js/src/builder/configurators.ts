export type Project<TValue, R = TValue> = ((value: TValue, indexPos: number, indexValue: number) => R);
export type IsNull<T> = ((value: T, key: number, index: number) => boolean);
export type GetKey<TKey, TValue> = ((key: TKey, value: TValue) => TValue | void);
// export type WriteValue<T, R> = (src: ArrayLike<T>, dst: R[], index: number) => boolean | void;
export type EncodeBinary<T = any> = ((value: T, key: number, index: number) => Iterable<number>);

export const identityTransform = (x : any) => x;
export const equalToNull = <T>(x: T) => x == null;
export const alignToBoundary = (x: number, to: number) => x + extra(x, to);
const extra = (x: number, to: number) => (x % to === 0 ? 0 : to - x % to);

export const utf8Encoder = TextEncoder.prototype.encode.bind(new TextEncoder());

export const HashSet = <TKey = any, TValue = any>() => {
    const hashMap = Object.create(null);

    return (key: TKey, value: TValue): TValue | void => {
        if (typeof value === "undefined") {
            // get value
            return hashMap[key];
        }
        else {
            // set value
            hashMap[key] = value;
        }
    };
}