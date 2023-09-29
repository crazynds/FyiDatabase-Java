package sgbd.query.agregation;

import engine.util.Util;
import sgbd.prototype.BData;
import sgbd.prototype.column.Column;
import sgbd.prototype.column.LongColumn;
import sgbd.prototype.metadata.LongMetadata;
import sgbd.prototype.query.Tuple;
import sgbd.prototype.query.fields.Field;



public class MaxAgregation extends AgregationOperation{
    protected Field value;

    protected Column meta;
    protected boolean fisrt = false;

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
        value = null;
        meta = null;
        fisrt = true;
    }

    @Override
    public void process(Tuple acumulator, Tuple newData) {
        Field f = newData.getContent(sourceSrc).getField(columnSrc);
        if(f==null)return;
        if(meta==null)meta = newData.getContent(sourceSrc).getMetadata(columnSrc);
        if(fisrt) {
            value = f;
            fisrt = false;
        }else if(value.compareTo(f)<0){
            value = f;
        }
    }

    @Override
    public void finalize(Tuple acumulator) {
        if(meta==null) {
            acumulator.getContent(sourceDst).setField(columnDst, value);
        }else{
            acumulator.getContent(sourceDst).setField(columnDst, value,meta);
        }
    }
}
