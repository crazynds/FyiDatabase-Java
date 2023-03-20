package sgbd.query.binaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.unaryop.DistinctOperator;
import sgbd.util.ComparableFilter;

public class OuterNeestedLoopJoin extends UnionOperator{

    public OuterNeestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(new LeftNestedLoopJoin(left,right,comparator), new RightNestedLoopJoin(left,right,comparator));
    }
}
