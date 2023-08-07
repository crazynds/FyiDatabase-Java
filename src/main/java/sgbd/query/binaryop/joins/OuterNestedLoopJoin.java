package sgbd.query.binaryop.joins;

import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.query.binaryop.UnionOperator;
import sgbd.util.interfaces.ComparableFilter;

public class OuterNestedLoopJoin extends UnionOperator {

    @Deprecated
    public OuterNestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(new LeftNestedLoopJoin(left,right,comparator), new RightNestedLoopJoin(left,right,comparator));
    }

}
