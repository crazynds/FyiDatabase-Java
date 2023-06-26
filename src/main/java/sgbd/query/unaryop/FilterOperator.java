package sgbd.query.unaryop;

import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.util.interfaces.Filter;

public class FilterOperator extends SimpleUnaryOperator {

    private Filter<Tuple> tupleFilter;

    public FilterOperator(Operator op, Filter<Tuple> tupleFilter) {
        super(op);
        this.tupleFilter = tupleFilter;
    }

    public Tuple getNextTuple(){
        while (operator.hasNext()){
            Tuple temp = operator.next();
            Query.COMPARE_FILTER++;
            if(tupleFilter.match(temp)) {
                return temp;
            }
        }
        return null;
    }

}
