package sgbd.query.binaryop;

import sgbd.info.Query;
import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.util.ComparableFilter;

import java.util.Map;

public class NestedLoopJoin extends BinaryOperator{

    protected Tuple nextTuple=null;
    protected Tuple currentLeftTuple=null;
    protected ComparableFilter<Tuple> comparator;

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
        return (nextTuple==null)?false:true;
    }

    protected Tuple findNextTuple(){
        //Executa apenas quando o next tuple não existe
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
                //Faz a comparação do join
                Query.COMPARE_JOIN++;
                if(comparator.match(currentLeftTuple,rightTuple)){
                    nextTuple = new Tuple();
                    for (Map.Entry<String, ComplexRowData> entry:
                            currentLeftTuple) {
                        nextTuple.setContent(entry.getKey(),entry.getValue());
                    }
                    for (Map.Entry<String, ComplexRowData> entry:
                            rightTuple) {
                        nextTuple.setContent(entry.getKey(),entry.getValue());
                    }
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
