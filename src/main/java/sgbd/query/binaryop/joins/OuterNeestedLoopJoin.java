package sgbd.query.binaryop.joins;

import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.binaryop.UnionOperator;
import sgbd.util.interfaces.ComparableFilter;

public class OuterNeestedLoopJoin extends UnionOperator {

    public OuterNeestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(new LeftNestedLoopJoin(left,right,comparator), new RightNestedLoopJoin(left,right,comparator));
    }
    public OuterNeestedLoopJoin(Operator left, Operator right) {
        super(new LeftNestedLoopJoin(left,right), new RightNestedLoopJoin(left,right));
    }
}
