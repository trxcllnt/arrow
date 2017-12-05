export abstract class ArrowType {
    abstract accept(visitor: ArrowTypeVisitor): any;
}

export class ArrowTypeVisitor {
    visit(node: ArrowType) {
        return node.accept(this);
    }
    visitMany(nodes: ArrowType[]): any[] {
        return nodes.map((node) => this.visit(node));
    }
}
