package sgbd.query.binaryop.joins;

import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.info.Query;
import sgbd.prototype.query.Tuple;
import sgbd.query.Operator;
import sgbd.query.binaryop.SimpleBinaryOperator;
import sgbd.util.interfaces.ComparableFilter;

public class NestedLoopJoin extends SimpleBinaryOperator {
    protected Tuple currentLeftTuple=null;
    protected BooleanExpression expression;
    protected ComparableFilter<Tuple> comparator;

    public NestedLoopJoin(Operator left, Operator right) {
        super(left, right);
        this.expression = null;
        this.comparator = null;
    }

    public NestedLoopJoin(Operator left, Operator right, BooleanExpression expression) {
        super(left, right);
        this.expression = expression;
        this.comparator = null;
    }

    @Deprecated
    public NestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparable) {
        super(left, right);
        this.expression = null;
        this.comparator = comparable;
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
                Tuple t = checkReturn(currentLeftTuple,rightTuple);
                if(t!=null)return t;
            }
            right.close();
            currentLeftTuple=null;
        }
        return null;
    }

    public Tuple checkReturn(Tuple left, Tuple right){
        Query.COMPARE_JOIN ++;
        Tuple custom = new Tuple(left,right);
        if(expression!=null){
            if(expression.solve(custom).val())
                return custom;
        }else if(comparator!=null){
            if(comparator.match(left,right))
                return custom;
        }else{
            return custom;
        }
        return null;
    }

}
