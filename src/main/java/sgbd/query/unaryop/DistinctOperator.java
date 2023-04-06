package sgbd.query.unaryop;

import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.binaryop.UnionOperator;

import java.util.ArrayList;

public class DistinctOperator extends UnaryOperator{

    private ArrayList<Tuple> tuples = new ArrayList<>();

    private Tuple nextTuple = null;
    public DistinctOperator(Operator op) {
        super(op);
    }


    private boolean checkValid(Tuple newTuple){
        for (Tuple t:
                tuples) {
            Query.COMPARE_DISTINCT_TUPLE++;
            if(t.compareTo(newTuple)==0 && newTuple.compareTo(t)==0)return false;
        }
        tuples.add(newTuple);
        return true;
    }

    private Tuple getNextTuple() {
        if (nextTuple != null) return nextTuple;
        while (operator.hasNext()) {
            nextTuple = operator.next();
            if (checkValid(nextTuple)) {
                return nextTuple;
            } else nextTuple = null;
        }
        return nextTuple;
    }

    @Override
    public void open() {
        tuples.clear();
        operator.open();

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
        operator.close();
    }
}
