package sgbd.query.basic.binaryop;

import sgbd.query.basic.Operator;

public abstract class BinaryOperator implements Operator {

    protected Operator left,right;
    public BinaryOperator(Operator left, Operator right){
        this.left=left;
        this.right=right;
    }

    public Operator getLeftOperator(){
        return left;
    }

    public Operator getRightOperator(){
        return right;
    }

    public void setLeftOperator(Operator op){
        this.left = op;
    }
    public void setRightOperator(Operator op){
        this.right = op;
    }

}
