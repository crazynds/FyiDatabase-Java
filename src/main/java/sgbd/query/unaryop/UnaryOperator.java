package sgbd.query.unaryop;

import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.query.Operator;
import sgbd.table.Table;

import java.util.List;
import java.util.Map;

public abstract class UnaryOperator implements Operator {

    protected Operator operator;

    @Override
    public void lookup(BooleanExpression expression) {
        // do nothing

    }

    public UnaryOperator(Operator op){
        setOperator(op);
    }

    public void setOperator(Operator op){
        this.operator=op;
    }

    public Operator getOperator() {
        return operator;
    }


    public List<Table> getSources(){
        return operator.getSources();
    }

    @Override
    public Map<String, List<String>> getContentInfo() {
        return operator.getContentInfo();
    }


    @Override
    public void open(){
        operator.open();
    }


    @Override
    public void close(){
        operator.close();
    }

    @Override
    public void freeResources(){
        operator.freeResources();
    }
}
