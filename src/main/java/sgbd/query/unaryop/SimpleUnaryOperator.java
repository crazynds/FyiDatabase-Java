package sgbd.query.unaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;

public abstract class SimpleUnaryOperator extends UnaryOperator{

    private Tuple lastTuple;
    public SimpleUnaryOperator(Operator op) {
        super(op);
    }

    @Override
    public void open() {
        super.open();
        lastTuple = null;
    }

    public abstract Tuple getNextTuple();
    private Tuple getNextCachedTuple(){
        if(lastTuple!=null)return lastTuple;
        lastTuple = getNextTuple();
        return lastTuple;
    }

    @Override
    public Tuple next() {
        try {
            return getNextCachedTuple();
        }finally {
            lastTuple = null;
        }
    }

    @Override
    public boolean hasNext() {
        return getNextCachedTuple()!=null;
    }

}
