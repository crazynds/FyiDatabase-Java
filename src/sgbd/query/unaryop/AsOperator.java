package sgbd.query.unaryop;

import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.util.Conversor;

public class AsOperator extends UnaryOperator {

    private Conversor conversor;
    private String name;

    public AsOperator(Operator op, Conversor conversor, String name) {
        super(op);
        this.conversor  = conversor;
        this.name       = name;
    }

    @Override
    public void open() {
        operator.open();
    }

    @Override
    public Tuple next() {
        Tuple t = operator.next();
        ComplexRowData row = t.getContent("asOperation");
        row.setData(name,conversor.process(t),conversor.metaInfo(t));
        return t;
    }

    @Override
    public boolean hasNext() {
        return operator.hasNext();
    }

    @Override
    public void close() {
        operator.close();
    }
}
