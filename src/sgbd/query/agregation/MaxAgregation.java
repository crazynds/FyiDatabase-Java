package sgbd.query.agregation;

import engine.util.Util;
import sgbd.prototype.Column;
import sgbd.query.Tuple;

import java.math.BigInteger;

public class MaxAgregation extends AgregationOperation{
    protected BigInteger number;

    protected Column meta;

    public MaxAgregation(String sourceSrc, String columnSrc, String sourceDst, String columnDst) {
        super(sourceSrc, columnSrc, sourceDst, columnDst);
    }

    public MaxAgregation(String sourceSrc, String columnSrc) {
        super(sourceSrc, columnSrc);
    }


    @Override
    public String getAgregationName() {
        return "max";
    }
    @Override
    public void initialize(Tuple acumulator) {
        number = BigInteger.ZERO;
        meta = null;
    }

    @Override
    public void process(Tuple acumulator, Tuple newData) {
        byte[] arr = newData.getContent(sourceSrc).getData(columnSrc);
        if(arr==null)return;
        if(meta==null)meta = newData.getContent(sourceSrc).getMeta(columnSrc);
        BigInteger check = Util.convertByteArrayToNumber(arr);
        if(number.compareTo(check)<0)number = check;
    }

    @Override
    public void finalize(Tuple acumulator) {
        byte[] arr = Util.convertNumberToByteArray(number,4);
        if(meta==null) {
            meta = new Column("__",(short)4,Column.SIGNED_INTEGER_COLUMN);
        }
        acumulator.getContent(sourceDst).setData(columnDst, arr,meta);
    }
}
