package sgbd.query.binaryop.joins;

import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.query.binaryop.UnionOperator;
import sgbd.util.interfaces.ComparableFilter;

public class OuterNestedLoopJoin extends UnionOperator {

    @Deprecated
    public OuterNestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(new LeftNestedLoopJoin(left,right,comparator), new RightNestedLoopJoin(left,right,comparator));
    }

    public OuterNestedLoopJoin(Operator left, Operator right, BooleanExpression expression) {
        super(new LeftNestedLoopJoin(left,right,expression), new RightNestedLoopJoin(left,right,expression));
    }

}
