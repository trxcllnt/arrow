import { Data } from '../data';
import { Int, Float16, Float32, Float64, Float } from '../type';
import { identityTransform, equalToNull, EncodeBinary, utf8Encoder, HashMap, Project, IsNull, GetKey } from './configurators';

export class DictionaryEncodeFloatBuilder<T extends number, TKey extends Int>
{
    private _keysCtor : new () => TKey;
    private _dataCtor : new () => Float16 | Float32 | Float64;
    private _project : Project<T, T>;
    private _isNull  : IsNull<T>;
    private _hashMap : GetKey<T, number>;
    private _encoder : EncodeBinary<T>;

    public constructor(
        keysCtor: new () => TKey,
        dataCtor: new () => Float16 | Float32 | Float64,
        project : Project<T, T>         = identityTransform,
        isNull  : IsNull<T>             = equalToNull,
        hashMap : GetKey<T, number>     = HashMap(),
        encoder : EncodeBinary<T>       = utf8Encoder)
    {
        this._keysCtor = keysCtor;
        this._dataCtor = dataCtor;
        this._project = project;
        this._isNull = isNull;
        this._hashMap = hashMap;
        this._encoder = encoder;
    }

    public encode(source: Iterable<T>) : { dictData : Data<Float>, keysData : Data<TKey> } {
        const indices = [-1];

        let null_count = 0;
        let key: number | void | boolean;
        let validity = [] as number[];
        let data = [] as T[];

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
            else if ((key = this._hashMap(value, indexValue)) !== undefined) {
                // else if a dictionary key exists, write the key into the current position in keys
                indices[sourceIndex] = key as number;
            }
            else {
                // otherwise, increment the key, encode the values as Floats
                indices[sourceIndex] = key = indexValue++;
                data.push(...(this._encoder(value, key, sourceIndex) as T[]));
            }

            sourceIndex++;
        }

        let validityUint8Array = null_count > 0 ? new Uint8Array(validity) : new Uint8Array(0);
        const dictData = Data.Float(new this._dataCtor(), 0, data.length, 0, null, data);
        const keysData = Data.Int(new this._keysCtor(), 0, indices.length, null_count, validityUint8Array, indices);

        return { dictData, keysData };
    }
}