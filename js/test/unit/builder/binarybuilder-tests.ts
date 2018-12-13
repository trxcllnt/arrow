import { DictionaryEncodeBinaryBuilder, Uint8, Vector } from '../../Arrow';

describe('DictionaryEncodeBinaryBuilder', () => {
    it('should encode an array as binary', () => {
        var builder = new DictionaryEncodeBinaryBuilder<Uint8['TValue'], Uint8>(Uint8);
        var valuesToEncode = new Uint8Array([11, 22, 33, 44, 55]);

        let {dictData} = builder.encode(valuesToEncode);

        let {values, valueOffsets} = Vector.new(dictData);
        let encodedValue = valuesToEncode.values();
        let offset = valueOffsets.values();
        let index = 0;

        for (let byte of values) {
            let o = offset.next();
            let e = encodedValue.next();

            expect(o.value).toBe(index + 1);
            expect(byte).toBe(e.value);
            
            index++;
        }
    });
});
