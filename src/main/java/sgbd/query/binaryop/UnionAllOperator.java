package sgbd.query.binaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.util.ArrayList;

public class UnionAllOperator extends SimpleBinaryOperator{

    private Tuple nextTuple = null;

    public UnionAllOperator(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public Tuple getNextTuple(){
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

}
