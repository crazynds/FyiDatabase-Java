package sgbd.query.basic.binaryop;

import sgbd.info.Query;
import sgbd.prototype.RowData;
import sgbd.query.basic.Operator;
import sgbd.query.basic.Tuple;
import sgbd.util.ComparableFilter;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Map;

public class BlockNestedLoopJoin extends NestedLoopJoin{

    private ArrayList<Tuple> bufferedLeftTuples=new ArrayList<>();
    private int indexLeftTuple;
    private int currentBufferedLeft = 0;
    private ArrayList<Tuple> bufferedRightTuples=new ArrayList<>();
    private int indexRightTuple;
    private int currentBufferedRight = 0;
    private int bufferSize = 4096;



    public BlockNestedLoopJoin(Operator left, Operator right,ComparableFilter<Tuple> comparator) {
        super(left, right,comparator);
    }

    @Override
    public void open() {
        bufferedLeftTuples.clear();
        indexLeftTuple = 0;

        bufferedRightTuples.clear();
        indexRightTuple = 0;

        super.open();
        right.open();
    }

    protected void prepareBuffers(){
        //Bufferiza o left
        while(bufferSize > currentBufferedLeft && left.hasNext()
            //    && bufferedLeftTuples.size()<3
        ){
            Tuple leftTuple = left.next();
            bufferedLeftTuples.add(leftTuple);
            currentBufferedLeft+=leftTuple.byteSize();
        }
        //bufferiza o right
        while(bufferSize > currentBufferedRight && right.hasNext()
            //    && bufferedRightTuples.size()<3
        ){
            Tuple rightTuple = right.next();
            bufferedRightTuples.add(rightTuple);
            currentBufferedRight+=rightTuple.byteSize();
        }

        if(bufferedRightTuples.size()==0){
            bufferedLeftTuples.clear();
            currentBufferedLeft = 0;
            right.close();
            right.open();
            if(!right.hasNext())return;
            else{
                prepareBuffers();
            }
        }
    }


    @Override
    protected Tuple findNextTuple() {
        if(nextTuple!=null)return nextTuple;

        //Chama o carregamento dos buffers
        prepareBuffers();

        while(indexLeftTuple<bufferedLeftTuples.size() && nextTuple==null){
            Tuple leftTuple = bufferedLeftTuples.get(indexLeftTuple);
            Tuple rightTuple = bufferedRightTuples.get(indexRightTuple++);
            Query.COMPARE_JOIN++;
            if (comparator.match(leftTuple,rightTuple)){
                nextTuple = new Tuple();
                for (Map.Entry<String, RowData> entry:
                        leftTuple) {
                    nextTuple.setContent(entry.getKey(),entry.getValue());
                }
                for (Map.Entry<String, RowData> entry:
                        rightTuple) {
                    nextTuple.setContent(entry.getKey(),entry.getValue());
                }
            }
            if(indexRightTuple>=bufferedRightTuples.size()){
                indexLeftTuple++;
                indexRightTuple=0;
                if(indexLeftTuple>=bufferedLeftTuples.size()){
                    bufferedRightTuples.clear();
                    currentBufferedRight = 0;
                    indexLeftTuple = 0;
                    if(nextTuple==null) {
                        nextTuple = findNextTuple();
                    }
                }
            }
        }
        return nextTuple;
    }
}
