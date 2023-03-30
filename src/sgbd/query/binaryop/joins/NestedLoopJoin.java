package sgbd.query.binaryop.joins;

import sgbd.info.Query;
import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.query.binaryop.BinaryOperator;
import sgbd.util.ComparableFilter;

import java.util.Map;

public class NestedLoopJoin extends BinaryOperator {

    protected Tuple nextTuple=null;
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
        left.open();
        nextTuple=null;
    }

    @Override
    public Tuple next() {
        try {
            if(nextTuple==null)findNextTuple();
            return nextTuple;
        }finally {
            nextTuple = null;
        }
    }

    @Override
    public boolean hasNext() {
        findNextTuple();
        return nextTuple!=null;
    }

    protected Tuple findNextTuple(){
        //Executa apenas quando o next tuple n�o existe
        if(nextTuple!=null)return nextTuple;
        //Loopa pelo operador esquerdo
        while(currentLeftTuple!=null || left.hasNext()){
            if(currentLeftTuple==null){
                currentLeftTuple = left.next();
                right.open();
            }
            //Loopa pelo operador direito
            while(right.hasNext()){
                Tuple rightTuple = right.next();
                //Faz a compara��o do join
                Query.COMPARE_JOIN++;
                if(comparator==null || comparator.match(currentLeftTuple,rightTuple)){
                    nextTuple = new Tuple(currentLeftTuple,rightTuple);
                    return nextTuple;
                }
            }
            right.close();
            currentLeftTuple=null;
        }
        return null;
    }

    @Override
    public void close() {
        nextTuple = null;
        left.close();
    }
}
