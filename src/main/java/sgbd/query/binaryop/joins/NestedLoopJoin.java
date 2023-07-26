package sgbd.query.binaryop.joins;

import enums.Result;
import lib.booleanexpression.entities.expressions.AtomicExpression;
import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.query.binaryop.SimpleBinaryOperator;
import sgbd.util.interfaces.ComparableFilter;

import java.util.Comparator;

public class NestedLoopJoin extends SimpleBinaryOperator {
    protected Tuple currentLeftTuple=null;
    protected BooleanExpression expression;
    protected ComparableFilter<Tuple> comparable;

    public NestedLoopJoin(Operator left, Operator right) {
        super(left, right);
        this.expression = null;
        this.comparable = null;
    }
    public NestedLoopJoin(Operator left, Operator right, BooleanExpression expression) {
        super(left, right);
        this.expression = expression;
        this.comparable = null;
    }
    public NestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparable) {
        super(left, right);
        this.expression = null;
        this.comparable = comparable;
    }

    @Override
    public void open() {
        super.open();
        right.close();
        currentLeftTuple = null;
    }

    public Tuple getNextTuple(){
        //Loopa pelo operador esquerdo
        while(currentLeftTuple!=null || left.hasNext()){
            if(currentLeftTuple==null){
                currentLeftTuple = left.next();
                right.lookup(expression);
                right.open();
            }
            //Loopa pelo operador direito
            while(right.hasNext()){
                Tuple rightTuple = right.next();
                //Faz a comparação do join
                Query.COMPARE_JOIN++;
                //if(expression==null || expression.solve() == Result.TRUE){
                //    return new Tuple(currentLeftTuple,rightTuple);
                //}
                if(comparable==null || comparable.match(currentLeftTuple,rightTuple)){
                    return new Tuple(currentLeftTuple,rightTuple);
                }
            }
            right.close();
            currentLeftTuple=null;
        }
        return null;
    }

}
