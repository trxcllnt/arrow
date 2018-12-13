import { Data } from '../data';
import { Binary, Int } from '../type';
import { identityTransform, equalToNull, EncodeBinary, utf8Encoder, HashMap, Project, IsNull, GetKey } from './configurators';

export class DictionaryEncodeBinaryBuilder<T, TKey extends Int>
{
    private _keysCtor : new () => TKey;
    private _project : Project<T, T>;
    private _isNull  : IsNull<T>;
    private _hashSet : GetKey<T, number>;
    private _encoder : EncodeBinary<T>;

    public constructor(
        keysCtor: new () => TKey,
        project : Project<T, T>         = identityTransform,
        isNull  : IsNull<T>             = equalToNull,
        hashSet : GetKey<T, number>     = HashMap(),
        encoder : EncodeBinary<T>       = (value: T) => utf8Encoder(value))
    {
        this._keysCtor = keysCtor;
        this._project = project;
        this._isNull = isNull;
        this._hashSet = hashSet;
        this._encoder = encoder;
    }

    public encode(source: Iterable<T>) : { dictData : Data<Binary>, keysData : Data<TKey> } {
        const indices = [-1], offsets = [0];

        let null_count = 0;
        let key: number | void | boolean;
        let validity = [] as number[];
        let data = [] as number[];

        let sourceIndex = 0;
        let indexValue = 0;
        for (let fromSource of source) {
            
            let value = this._project(fromSource, indexValue, sourceIndex);

            let validityIndex = sourceIndex >> 3;
            if (validity.length >= validityIndex) {
                validity[validityIndex] = 0;
            }

            if (this._isNull(value, sourceIndex, indexValue)) {
                // if null, increment null count and write pos in nullBbitmap
                null_count++;
                indices[sourceIndex] = -1;
                validity[validityIndex] &= ~(1 << (sourceIndex % 8));
            }
            else if ((key = this._hashSet(value, indexValue)) !== undefined) {
                // else if a dictionary key exists, write the key into the current position in keys
                indices[sourceIndex] = key as number;
            }
            else {
                // otherwise, increment the key, encode the values as binary, and add the offsets
                indices[sourceIndex] = key = indexValue++;
                var encodedValues = this._encoder(value, key, sourceIndex);
                offsets[sourceIndex] = data.push(...(encodedValues as number[]));
            }

            sourceIndex++;
        }

        let validityUint8Array = null_count > 0 ? new Uint8Array(validity) : new Uint8Array(0);
        const dictData = Data.Binary(new Binary(), 0, offsets.length - 1, 0, null, new Uint8Array(data), new Uint8Array(offsets));
        const keysData = Data.Int(new this._keysCtor(), 0, indices.length, null_count, validityUint8Array, indices);

        return { dictData, keysData };
    }
}