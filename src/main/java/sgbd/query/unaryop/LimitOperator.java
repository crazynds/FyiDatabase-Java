package sgbd.query.unaryop;

import sgbd.query.Operator;
import sgbd.query.Tuple;

public class LimitOperator extends SimpleUnaryOperator{

    private long qtd, limit;

    public LimitOperator(Operator op,long limit) {
        super(op);
        this.limit = limit;
    }

    @Override
    public void open() {
        super.open();
        qtd = 0;
    }

    @Override
    public Tuple getNextTuple() {
        if(qtd>=limit)return null;
        qtd++;
        return operator.next();
    }
}
