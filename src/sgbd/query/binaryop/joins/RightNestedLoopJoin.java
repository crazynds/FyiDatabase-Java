package sgbd.query.binaryop.joins;

import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.util.ComparableFilter;

public class RightNestedLoopJoin extends LeftNestedLoopJoin{
    public RightNestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(right, left, comparator); // apenas inverte os operadores
    }
    public RightNestedLoopJoin(Operator left, Operator right) {
        super(right, left); // apenas inverte os operadores
    }
}
