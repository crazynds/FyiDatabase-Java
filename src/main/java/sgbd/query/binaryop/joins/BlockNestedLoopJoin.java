package sgbd.query.binaryop.joins;

import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.util.interfaces.ComparableFilter;

import java.util.ArrayList;

public class BlockNestedLoopJoin extends NestedLoopJoin{

    private ArrayList<Tuple> bufferedLeftTuples=new ArrayList<>();
    private int indexLeftTuple;
    private long currentBufferedLeft = 0;

    private Tuple rightTuple=null;

    private final long bufferSize = 4096;
    private final int maxBufferedTuples = 10;


    @Deprecated
    public BlockNestedLoopJoin(Operator left, Operator right,ComparableFilter<Tuple> comparator) {
        super(left, right,comparator);
    }

    public BlockNestedLoopJoin(Operator left, Operator right, BooleanExpression expression) {
        super(left, right,expression);
    }

    public BlockNestedLoopJoin(Operator left, Operator right) {
        super(left, right);
    }

    @Override
    public void open() {
        bufferedLeftTuples.clear();
        indexLeftTuple = 0;
        currentBufferedLeft = 0;

        rightTuple= null;

        super.open();
        right.open();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple leftTuple,t = null;

        //Bufferiza o left
        while(bufferSize > currentBufferedLeft
                && bufferedLeftTuples.size()<maxBufferedTuples
                && left.hasNext()
        ){
            leftTuple = left.next();
            bufferedLeftTuples.add(leftTuple);
            currentBufferedLeft+=leftTuple.byteSize();
        }

        while(!bufferedLeftTuples.isEmpty()){
            if(rightTuple==null){
                if(right.hasNext()){
                    indexLeftTuple = 0;
                    rightTuple=right.next();
                }else{
                    right.close();
                    right.open();
                    if(right.hasNext()==false){
                        rightTuple = null;
                        return null;
                    }
                    bufferedLeftTuples.clear();
                    currentBufferedLeft = 0;
                    indexLeftTuple = 0;
                    return getNextTuple();
                }
            }
            while(bufferedLeftTuples.size()>indexLeftTuple) {
                leftTuple = bufferedLeftTuples.get(indexLeftTuple++);
                t = checkReturn(leftTuple,rightTuple);
                if(t!=null)return t;
            }
            rightTuple= null;
        }
        return null;
    }
}
