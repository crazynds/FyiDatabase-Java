package sgbd.query.binaryop.joins;

import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.binaryop.BinaryOperator;
import sgbd.query.binaryop.SimpleBinaryOperator;
import sgbd.util.interfaces.ComparableFilter;

public class NestedLoopJoin extends SimpleBinaryOperator {
    protected Tuple currentLeftTuple=null;
    protected ComparableFilter<Tuple> comparator;

    public NestedLoopJoin(Operator left, Operator right) {
        super(left, right);
        this.comparator = null;
    }
    public NestedLoopJoin(Operator left, Operator right, ComparableFilter<Tuple> comparator) {
        super(left, right);
        this.comparator = comparator;
    }

    @Override
    public void open() {
        super.open();
        right.close();
    }

    public Tuple getNextTuple(){
        //Loopa pelo operador esquerdo
        while(currentLeftTuple!=null || left.hasNext()){
            if(currentLeftTuple==null){
                currentLeftTuple = left.next();
                right.open();
            }
            //Loopa pelo operador direito
            while(right.hasNext()){
                Tuple rightTuple = right.next();
                //Faz a comparação do join
                Query.COMPARE_JOIN++;
                if(comparator==null || comparator.match(currentLeftTuple,rightTuple)){
                    return new Tuple(currentLeftTuple,rightTuple);
                }
            }
            right.close();
            currentLeftTuple=null;
        }
        return null;
    }

}
