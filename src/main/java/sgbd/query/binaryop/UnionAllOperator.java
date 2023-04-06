package sgbd.query.binaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.util.ArrayList;

public class UnionAllOperator extends BinaryOperator{

    private Tuple nextTuple = null;

    public UnionAllOperator(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public void open() {
        left.open();
        right.open();
    }


    private Tuple getNextTuple(){
        if(nextTuple != null)return nextTuple;
        while(left.hasNext()){
            nextTuple = left.next();
            return nextTuple;
        }
        while(right.hasNext()){
            nextTuple = right.next();
            return nextTuple;
        }
        return nextTuple;
    }

    @Override
    public Tuple next() {
        Tuple t = getNextTuple();
        nextTuple = null;
        return t;
    }

    @Override
    public boolean hasNext() {
        return getNextTuple() != null;
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }
}
