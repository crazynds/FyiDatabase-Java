package sgbd.query.binaryop.joins;

import engine.util.Util;
import lib.BigKey;
import sgbd.info.Query;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;
import sgbd.query.binaryop.SimpleBinaryOperator;
import sgbd.query.unaryop.SortOperator;
import sgbd.query.unaryop.UnaryOperator;
import sgbd.util.classes.ResourceName;


import java.util.Map;

public class MergeJoin extends SimpleBinaryOperator {

    protected String leftSource,leftData,rightSource,rightData;

    Map.Entry<Tuple, BigKey> leftCurrent,rightCurrent;

    public MergeJoin(Operator left, Operator right, ResourceName leftRes, ResourceName rightRes) {
        super(
            new SortOperator(left,leftRes),
            new SortOperator(right,rightRes)
        );
        this.leftSource=leftRes.getSource();
        this.leftData=leftRes.getColumn();
        this.rightSource=rightRes.getSource();
        this.rightData=rightRes.getColumn();
    }

    @Override
    public void setLeftOperator(Operator op) {
        ((UnaryOperator)left).setOperator(op);
    }

    @Override
    public void setRightOperator(Operator op){
        ((UnaryOperator)right).setOperator(op);
    }

    @Override
    public Operator getLeftOperator() {
        return ((UnaryOperator)left).getOperator();
    }

    @Override
    public Operator getRightOperator() {
        return ((UnaryOperator)right).getOperator();
    }


    protected Map.Entry<Tuple,BigKey> nextRight(){
        return nextSide(right, rightSource, rightData);
    }

    protected Map.Entry<Tuple,BigKey> nextLeft(){
        return nextSide(left, leftSource, leftData);
    }

    private Map.Entry<Tuple, BigKey> nextSide(Operator op, String rightSource, String rightData) {
        if(!op.hasNext())return null;
        return new Map.Entry<Tuple, BigKey>() {
            Tuple tuple = op.next();
            BigKey val = new BigKey(tuple.getContent(rightSource).getData(rightData));

            @Override
            public Tuple getKey() {
                return tuple;
            }

            @Override
            public BigKey getValue() {
                return val;
            }

            @Override
            public BigKey setValue(BigKey value) {
                return val;
            }
        };
    }

    public Tuple getNextTuple(){
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
                    Tuple tuple = new Tuple(leftCurrent.getKey(),rightCurrent.getKey());
                    leftCurrent = null;
                    return tuple;
                case -1:
                    leftCurrent = nextLeft();
                    break;
            }
        }while(leftCurrent !=null && rightCurrent!=null);
        return null;
    }

}
