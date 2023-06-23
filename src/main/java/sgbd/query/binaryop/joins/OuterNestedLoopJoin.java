package sgbd.query.binaryop.joins;

import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.query.binaryop.UnionOperator;
import sgbd.util.interfaces.ComparableFilter;

public class OuterNestedLoopJoin extends UnionOperator {

    public OuterNestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(new LeftNestedLoopJoin(left,right,comparator), new RightNestedLoopJoin(left,right,comparator));
    }
    public OuterNestedLoopJoin(Operator left, Operator right) {
        super(new LeftNestedLoopJoin(left,right), new RightNestedLoopJoin(left,right));
    }
}
