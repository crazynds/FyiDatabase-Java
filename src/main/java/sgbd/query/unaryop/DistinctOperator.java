package sgbd.query.unaryop;

import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;

public class DistinctOperator extends SimpleUnaryOperator{

    Tuple lastTuple = null;

    public DistinctOperator(Operator op) {
        super(new SortOperator(op));
    }


    private boolean checkValid(Tuple newTuple){
        if(lastTuple==null)return true;
        Query.COMPARE_DISTINCT_TUPLE++;
        if(lastTuple.compareTo(newTuple)==0 && newTuple.compareTo(lastTuple)==0)return false;
        return true;
    }

    public Tuple getNextTuple() {
        while (operator.hasNext()) {
            Tuple t = operator.next();
            if (checkValid(t)) {
                lastTuple = t;
                return t;
            }
        }
        return null;
    }

    @Override
    public void open() {
        super.open();
    }

}
