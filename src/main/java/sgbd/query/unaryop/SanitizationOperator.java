package sgbd.query.unaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.util.interfaces.Sanitization;

public class SanitizationOperator extends UnaryOperator {

    private Sanitization sanitization;

    public SanitizationOperator(Operator op, Sanitization sanitization) {
        super(op);
        this.sanitization = sanitization;
    }

    @Override
    public Tuple next() {
        Tuple t = operator.next();
        if(t==null)return null;
        return sanitization.sanitize(t.clone());
    }

    @Override
    public boolean hasNext() {
        return operator.hasNext();
    }

}
