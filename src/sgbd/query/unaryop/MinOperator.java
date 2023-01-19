package sgbd.query.unaryop;

import engine.util.Util;
import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.math.BigInteger;

public class MinOperator extends MaxOperator{
    public MinOperator(Operator op, String source, String column) {
        super(op, source, column);
    }

    @Override
    protected void checkValues(Tuple val) {
        byte[] aux =val.getContent(source).getData(column);
        BigInteger bg = Util.convertByteArrayToNumber(aux);
        if(maxValue == null){
            maxValue = bg;
            data = aux;
            meta = val.getContent(source).getMeta(column);
        }else if(bg.compareTo(maxValue)<0){
            maxValue = bg;
            data = aux;
        }
    }

    @Override
    protected String getAsColumn() {
        return "min("+column+")";
    }
}
