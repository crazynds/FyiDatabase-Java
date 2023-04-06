package sgbd.query.binaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.util.List;

public class IntersectionOperator extends UnionOperator{
    public IntersectionOperator(Operator left, Operator right) {
        super(left, right);
    }

    public IntersectionOperator(Operator left, Operator right, List<String> leftColumns, List<String> rightColumns) {
        super(left, right, leftColumns, rightColumns);
    }


    @Override
    protected Tuple getNextTuple(){
        if(nextTuple != null)return nextTuple;
        while(left.hasNext()){
            nextTuple = left.next();
            tuples.add(nextTuple);
        }
        while(right.hasNext()){
            nextTuple = right.next();
            isRight = true;
            if(!checkValid(nextTuple)){
                return nextTuple;
            }else nextTuple = null;
        }
        return nextTuple;
    }
}
