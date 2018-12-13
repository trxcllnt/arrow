import { Uint32, DictionaryEncodeBinaryBuilder } from '../../Arrow';

describe('DictionaryEncodeBinaryBuilder', () => {
    it('should encode an array as binary', () => {
        var builder = new DictionaryEncodeBinaryBuilder<Number, Uint32>(Uint32);
        var values = [11, 22, 33, 44, 55];

        let {dictData, keysData} = builder.encode(values);

        console.log(dictData);
        console.log(keysData);
    });
});
