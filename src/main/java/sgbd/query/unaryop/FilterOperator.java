package sgbd.query.unaryop;

import lib.booleanexpression.entities.AttributeFilters;
import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.util.interfaces.Filter;

public class FilterOperator extends SimpleUnaryOperator {

    private Filter<Tuple> tupleFilter;
    private BooleanExpression expression = null;


    @Deprecated
    public FilterOperator(Operator op, Filter<Tuple> tupleFilter) {
        super(op);
        this.tupleFilter = tupleFilter;
    }

    public FilterOperator(Operator op, BooleanExpression expression){
        super(op);
        this.expression = expression;
    }

    @Override
    public void lookup(AttributeFilters filters) {
        expression.applyAttributeFilters(filters);
        operator.lookup(filters);
    }

    @Override
    public void open() {
        AttributeFilters filters = new AttributeFilters();
        this.lookup(filters);
        super.open();
    }

    public Tuple getNextTuple(){
        while (operator.hasNext()){
            Tuple temp = operator.next();
            Query.COMPARE_FILTER++;
            if (this.expression != null) {
                if(this.expression.solve(temp).val())return temp;
            }else if(tupleFilter.match(temp)) {
                return temp;
            }
        }
        return null;
    }

}
