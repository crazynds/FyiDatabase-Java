package sgbd.query.unaryop;

import engine.util.Util;
import sgbd.prototype.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.math.BigInteger;

public class MaxOperator extends UnaryOperator {
    protected BigInteger maxValue = null;
    protected byte[] data;
    protected Column meta;
    protected String source,column;

    public MaxOperator(Operator op,String source, String column) {
        super(op);
        this.source = source;
        this.column = column;
    }

    protected void checkValues(Tuple val){
        byte[] aux =val.getContent(source).getData(column);
        BigInteger bg = Util.convertByteArrayToNumber(aux);
        if(maxValue == null){
            maxValue = bg;
            data = aux;
            meta = val.getContent(source).getMeta(column);
        }else if(bg.compareTo(maxValue)>0){
            maxValue = bg;
            data = aux;
        }
    }


    @Override
    public void open() {
        maxValue = null;
        data = null;
        operator.open();
        while (operator.hasNext()){
            Tuple e = operator.next();
            checkValues(e);
        }
        operator.close();
    }

    @Override
    public Tuple next() {
        if(data==null)return null;
        Tuple t = new Tuple();
        ComplexRowData complexRowData = new ComplexRowData();
        complexRowData.setData("max("+column+")",data,meta);
        t.setContent(source,complexRowData);
        data = null;
        maxValue = null;
        return t;
    }

    @Override
    public boolean hasNext() {
        return maxValue!=null;
    }

    @Override
    public void close() {
        maxValue = null;
        data = null;
    }
}
