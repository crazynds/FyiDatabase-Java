package sgbd.query.binaryop.joins;

import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.util.interfaces.ComparableFilter;

public class LeftNestedLoopJoin extends NestedLoopJoin{

    protected boolean findAnyMatch = false;

    @Deprecated
    public LeftNestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(left, right, comparator);
    }

    public LeftNestedLoopJoin(Operator left, Operator right, BooleanExpression expression) {
        super(left, right, expression);
    }

    @Override
    public void open() {
        super.open();
        findAnyMatch = false;
    }

    @Override
    public Tuple getNextTuple(){
        //Loopa pelo operador esquerdo
        while(currentLeftTuple!=null || left.hasNext()){
            if(currentLeftTuple==null){
                currentLeftTuple = left.next();
                right.open();
                findAnyMatch = false;
            }
            //Loopa pelo operador direito
            while(right.hasNext()){
                Tuple rightTuple = right.next();
                //Faz a comparação do join
                Tuple t =checkReturn(currentLeftTuple,rightTuple);
                if(t!=null){
                    findAnyMatch |= true;
                    return t;
                }
            }
            right.close();
            if(!findAnyMatch){
                Tuple t = currentLeftTuple;
                currentLeftTuple = null;
                return t;
            }else currentLeftTuple=null;
        }
        return null;
    }
}
