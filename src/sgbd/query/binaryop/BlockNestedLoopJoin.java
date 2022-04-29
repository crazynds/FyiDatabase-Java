package sgbd.query.binaryop;

import sgbd.info.Query;
import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;
import sgbd.util.ComparableFilter;

import java.util.ArrayList;
import java.util.Map;

public class BlockNestedLoopJoin extends NestedLoopJoin{

    private ArrayList<Tuple> bufferedLeftTuples=new ArrayList<>();
    private int indexLeftTuple;
    private int currentBufferedLeft = 0;

    private Tuple rightTuple=null;

    private int bufferSize = 4096;



    public BlockNestedLoopJoin(Operator left, Operator right,ComparableFilter<Tuple> comparator) {
        super(left, right,comparator);
    }

    @Override
    public void open() {
        bufferedLeftTuples.clear();
        indexLeftTuple = 0;

        rightTuple= null;

        super.open();
        right.open();
    }

    @Override
    protected Tuple findNextTuple() {
        if(nextTuple!=null)return nextTuple;
        Tuple leftTuple;

        //Bufferiza o left
        while(bufferSize > currentBufferedLeft && left.hasNext()
                && bufferedLeftTuples.size()<3
        ){
            leftTuple = left.next();
            bufferedLeftTuples.add(leftTuple);
            currentBufferedLeft+=leftTuple.byteSize();
        }

        while(nextTuple==null && !bufferedLeftTuples.isEmpty()){
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
                    return findNextTuple();
                }
            }
            if(bufferedLeftTuples.size()>indexLeftTuple) {
                leftTuple = bufferedLeftTuples.get(indexLeftTuple++);
                Query.COMPARE_JOIN++;
                if(comparator.match(leftTuple,rightTuple)) {
                    nextTuple = new Tuple();
                    for (Map.Entry<String, ComplexRowData> entry:
                            leftTuple) {
                        nextTuple.setContent(entry.getKey(),entry.getValue());
                    }
                    for (Map.Entry<String, ComplexRowData> entry:
                            rightTuple) {
                        nextTuple.setContent(entry.getKey(),entry.getValue());
                    }
                }
            }else{
                rightTuple= null;
            }
        }
        return nextTuple;
    }
}
