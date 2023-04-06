package sgbd.query.unaryop;

import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.util.Filter;

public class FilterOperator extends UnaryOperator {

    private Filter<Tuple> tupleFilter;

    private Tuple nextTuple;

    public FilterOperator(Operator op, Filter<Tuple> tupleFilter) {
        super(op);
        this.tupleFilter=tupleFilter;
    }

    @Override
    public void open() {
        operator.open();
        nextTuple=null;
    }

    @Override
    public Tuple next() {
        try {
            return findNextTuple();
        }finally {
            nextTuple = null;
        }
    }

    @Override
    public boolean hasNext() {
        if(findNextTuple()!=null)return true;
        return false;
    }

    private Tuple findNextTuple(){
        if(nextTuple!=null)return nextTuple;

        while (operator.hasNext()){
            Tuple temp = operator.next();
            Query.COMPARE_FILTER++;
            if(tupleFilter.match(temp)) {
                nextTuple = temp;
                return nextTuple;
            }
        }
        return null;
    }

    @Override
    public void close() {
        operator.close();
        nextTuple=null;
    }
}
