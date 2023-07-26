package sgbd.query.binaryop.joins;

import enums.Result;
import lib.booleanexpression.entities.expressions.AtomicExpression;
import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.query.binaryop.SimpleBinaryOperator;
import sgbd.util.interfaces.ComparableFilter;

public class NestedLoopJoin extends SimpleBinaryOperator {
    protected Tuple currentLeftTuple=null;
    protected BooleanExpression expression;

    public NestedLoopJoin(Operator left, Operator right) {
        super(left, right);
        this.expression = null;
    }
    public NestedLoopJoin(Operator left, Operator right, BooleanExpression expression) {
        super(left, right);
        this.expression = expression;
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
                if(expression==null || expression.solve() == Result.TRUE){
                    return new Tuple(currentLeftTuple,rightTuple);
                }
            }
            right.close();
            currentLeftTuple=null;
        }
        return null;
    }

}
