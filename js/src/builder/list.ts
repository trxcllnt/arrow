import { DataType, List } from '../type';
import { NestedBuilder } from './base';

export class ListBuilder<T extends DataType = any, TNull = any> extends NestedBuilder<List<T>, TNull> {
    private row = new RowLike<T, TNull>();
    public writeValue(value: any, offset: number) {
        const row = this.row;
        row.values = value;
        super.writeValue(row as any, offset);
        row.values = null;
    }
}

class RowLike<T extends DataType = any, TNull = any> {
    public values: null | ArrayLike<T['TValue'] | TNull> = null;
    get(index: number) {
        return this.values ? this.values[index] : null;
    }
}
