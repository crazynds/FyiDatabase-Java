package sgbd.query.unaryop;

import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;

import java.util.ArrayList;

public class DistinctOperator extends SimpleUnaryOperator{

    private ArrayList<Tuple> tuples = new ArrayList<>();

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

    public Tuple getNextTuple() {
        while (operator.hasNext()) {
            Tuple t = operator.next();
            if (checkValid(t)) {
                return t;
            }
        }
        return null;
    }

    @Override
    public void open() {
        super.open();
        tuples.clear();
    }

}
