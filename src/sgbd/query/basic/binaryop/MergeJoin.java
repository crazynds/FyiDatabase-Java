package sgbd.query.basic.binaryop;

import engine.util.Util;
import sgbd.info.Query;
import sgbd.prototype.ComplexRowData;
import sgbd.prototype.RowData;
import sgbd.query.basic.Operator;
import sgbd.query.basic.Tuple;
import sgbd.query.basic.unaryop.SortOperator;

import java.math.BigInteger;
import java.util.Map;

public class MergeJoin extends BinaryOperator{
    protected Tuple nextTuple=null;

    protected String leftSource,leftData,rightSource,rightData;

    Map.Entry<Tuple,BigInteger> leftCurrent,rightCurrent;

    public MergeJoin(Operator left, Operator right,String leftSource, String leftData, String rightSource,String rightData) {
        super(
            new SortOperator(left,(o1, o2) -> {
                BigInteger int1 = Util.convertByteArrayToNumber(o1.getContent(leftSource).getData(leftData));
                BigInteger int2 = Util.convertByteArrayToNumber(o2.getContent(leftSource).getData(leftData));
                return int1.compareTo(int2);
            }),
            new SortOperator(right,(o1, o2) -> {
                BigInteger int1 = Util.convertByteArrayToNumber(o1.getContent(rightSource).getData(rightData));
                BigInteger int2 = Util.convertByteArrayToNumber(o2.getContent(rightSource).getData(rightData));
                return int1.compareTo(int2);
            })
        );
        this.leftSource=leftSource;
        this.leftData=leftData;
        this.rightSource=rightSource;
        this.rightData=rightData;
    }

    @Override
    public void open() {
        left.open();
        right.open();
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

    protected Map.Entry<Tuple,BigInteger> nextRight(){
        if(!right.hasNext())return null;
        return new Map.Entry<Tuple, BigInteger>() {
            Tuple tuple = right.next();
            BigInteger val = Util.convertByteArrayToNumber(tuple.getContent(rightSource).getData(rightData));

            @Override
            public Tuple getKey() {
                return tuple;
            }

            @Override
            public BigInteger getValue() {
                return val;
            }

            @Override
            public BigInteger setValue(BigInteger value) {
                return val;
            }
        };
    }
    protected Map.Entry<Tuple,BigInteger> nextLeft(){
        if(!left.hasNext())return null;
        return new Map.Entry<Tuple, BigInteger>() {
            Tuple tuple = left.next();
            BigInteger val = Util.convertByteArrayToNumber(tuple.getContent(leftSource).getData(leftData));

            @Override
            public Tuple getKey() {
                return tuple;
            }

            @Override
            public BigInteger getValue() {
                return val;
            }

            @Override
            public BigInteger setValue(BigInteger value) {
                return val;
            }
        };
    }

    protected Tuple findNextTuple(){
        if(nextTuple!=null)return nextTuple;
        if(leftCurrent==null)
            leftCurrent = nextLeft();
        if(leftCurrent==null)return null;
        if(rightCurrent==null)
            rightCurrent = nextRight();
        if(rightCurrent==null)return null;

        do {
            Query.COMPARE_JOIN++;
            switch (leftCurrent.getValue().compareTo(rightCurrent.getValue())) {
                case 1:
                    rightCurrent = nextRight();
                    break;
                case 0:
                    Tuple tuple = leftCurrent.getKey();
                    for (Map.Entry<String, ComplexRowData> entry :
                            rightCurrent.getKey()) {
                        tuple.setContent(entry.getKey(), entry.getValue());
                    }
                    leftCurrent = null;
                    nextTuple = tuple;
                    return nextTuple;
                case -1:
                    leftCurrent = nextLeft();
                    break;
            }
        }while(leftCurrent !=null && rightCurrent!=null);
        return null;
    }

    @Override
    public void close() {
        nextTuple = null;
    }
}
