package sgbd.query.binaryop;

import sgbd.query.Operator;
import sgbd.table.Table;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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


    public List<Table> getSources(){
        List<Table> rTable = right.getSources();
        List<Table> lTable = left.getSources();
        return Stream.concat(rTable.stream(),lTable.stream()).collect(Collectors.toList());
    }

}
