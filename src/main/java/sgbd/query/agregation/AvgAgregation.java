package sgbd.query.agregation;

import engine.util.Util;
import sgbd.prototype.Column;
import sgbd.query.Tuple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

public class AvgAgregation extends AgregationOperation{
    private double sum;
    private int qtd;

    public AvgAgregation(String sourceSrc, String columnSrc, String sourceDst, String columnDst) {
        super(sourceSrc, columnSrc, sourceDst, columnDst);
    }

    public AvgAgregation(String sourceSrc, String columnSrc) {
        super(sourceSrc, columnSrc);
    }


    @Override
    public String getAgregationName() {
        return "avg";
    }

    @Override
    public void initialize(Tuple acumulator) {
        sum = 0;
        qtd = 0;
    }

    @Override
    public void process(Tuple acumulator, Tuple newData){
        Column meta = newData.getContent(sourceSrc).getMeta(columnSrc);
        if(meta==null)return;
        if(meta.isInt()){
            sum += newData.getContent(sourceSrc).getInt(columnSrc);
        }else if(meta.isFloat()){
            sum += newData.getContent(sourceSrc).getFloat(columnSrc);
        }else if(meta.isDouble()){
            sum += newData.getContent(sourceSrc).getDouble(columnSrc);
        }else {
            sum += 0;
        }
        qtd++;
    }

    @Override
    public void finalize(Tuple acumulator) {
        double result = (qtd>0)?(sum / qtd):0;
        acumulator.getContent(sourceDst).setDouble(columnDst,result,new Column("avg",(short)8,Column.FLOATING_POINT));
    }

}
