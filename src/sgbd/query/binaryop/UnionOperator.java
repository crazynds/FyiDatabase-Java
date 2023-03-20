package sgbd.query.binaryop;

import engine.exceptions.DataBaseException;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.unaryop.DistinctOperator;
import sgbd.util.ComparableFilter;

import java.util.ArrayList;
import java.util.List;

public class UnionOperator extends BinaryOperator{

    private ArrayList<Tuple> tuples = new ArrayList<>();
    private ComparableFilter<Tuple> comparator;

    private Tuple nextTuple = null;

    public UnionOperator(Operator left, Operator right) {
        super(left, right);
        comparator = (t1, t2) -> {
            return t1.compareTo(t2) == 0 && t2.compareTo(t1) == 0;
        };
    }
    public UnionOperator(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(new DistinctOperator(left), new DistinctOperator(right));
        this.comparator = comparator;
    }

    @Override
    public void open() {
        tuples.clear();
        left.open();
        right.open();
    }

    private boolean checkValid(Tuple newTuple){
        for (Tuple t:
             tuples) {
            if(comparator.match(newTuple,t))return false;
        }
        return true;
    }

    private Tuple getNextTuple(){
        if(nextTuple != null)return nextTuple;
        while(left.hasNext()){
            nextTuple = left.next();
            tuples.add(nextTuple);
            return nextTuple;
        }
        while(right.hasNext()){
            nextTuple = right.next();
            if(checkValid(nextTuple)){
                return nextTuple;
            }else nextTuple = null;
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
        tuples.clear();
        left.close();
        right.close();
    }
}
