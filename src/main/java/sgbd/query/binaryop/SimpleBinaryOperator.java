package sgbd.query.binaryop;

import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;

public abstract class SimpleBinaryOperator extends BinaryOperator{

    private Tuple lastTuple;
    public SimpleBinaryOperator(Operator left, Operator right) {
        super(left, right);
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
