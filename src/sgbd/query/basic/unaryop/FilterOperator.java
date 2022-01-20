package sgbd.query.basic.unaryop;

import sgbd.info.Query;
import sgbd.query.basic.Operator;
import sgbd.query.basic.Tuple;
import sgbd.util.Filter;

public class FilterOperator extends UnaryOperation {

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
            Query.FILTER++;
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
