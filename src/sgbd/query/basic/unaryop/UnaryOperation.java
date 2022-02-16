package sgbd.query.basic.unaryop;

import sgbd.query.basic.Operator;
import sgbd.table.Table;

import java.util.List;

public abstract class UnaryOperation implements Operator {

    protected Operator operator;

    public UnaryOperation(Operator op){
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
}
