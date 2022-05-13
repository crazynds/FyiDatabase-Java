package sgbd.query.unaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.util.Sanitization;

public class SanitizationOperator extends UnaryOperator {

    private Sanitization sanitization;

    public SanitizationOperator(Operator op, Sanitization sanitization) {
        super(op);
        this.sanitization = sanitization;
    }

    @Override
    public void open() {
        operator.open();
    }

    @Override
    public Tuple next() {
        Tuple t = operator.next();
        if(t==null)return null;
        return sanitization.sanitize(t);
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
