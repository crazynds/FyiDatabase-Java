package sgbd.query.binaryop;

import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;

import java.util.List;

public class IntersectionOperator extends UnionOperator{
    public IntersectionOperator(Operator left, Operator right) {
        super(left, right);
    }

    public IntersectionOperator(Operator left, Operator right, List<String> leftColumns, List<String> rightColumns) {
        super(left, right, leftColumns, rightColumns);
    }


    @Override
    public Tuple getNextTuple(){
        Tuple t = null;
        while(left.hasNext()){
            t = left.next();
            tuples.add(t);
        }
        while(right.hasNext()){
            t = right.next();
            isRight = true;
            if(!checkValid(t)){
                return t;
            }else t = null;
        }
        return t;
    }
}
