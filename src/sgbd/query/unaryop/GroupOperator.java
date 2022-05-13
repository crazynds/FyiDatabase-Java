package sgbd.query.unaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;

public class GroupOperator extends UnaryOperation{
    public GroupOperator(Operator op) {
        super(op);
    }

    @Override
    public void open() {

    }

    @Override
    public Tuple next() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void close() {

    }
}
