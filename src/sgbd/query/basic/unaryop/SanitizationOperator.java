package sgbd.query.basic.unaryop;

import sgbd.query.basic.Operator;
import sgbd.query.basic.Tuple;
import sgbd.util.Sanitization;

public class SanitizationOperator extends UnaryOperation{

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
