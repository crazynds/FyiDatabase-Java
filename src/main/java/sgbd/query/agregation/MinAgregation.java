package sgbd.query.agregation;

import engine.util.Util;
import sgbd.prototype.query.Tuple;
import sgbd.prototype.query.fields.Field;



public class MinAgregation extends MaxAgregation{

    public MinAgregation(String sourceSrc, String columnSrc, String sourceDst, String columnDst) {
        super(sourceSrc, columnSrc, sourceDst, columnDst);
    }

    public MinAgregation(String sourceSrc, String columnSrc) {
        super(sourceSrc, columnSrc);
    }

    @Override
    public void initialize(Tuple acumulator) {
        super.initialize(acumulator);
    }

    @Override
    public String getAgregationName() {
        return "min";
    }

    @Override
    public void process(Tuple acumulator, Tuple newData) {
        Field f = newData.getContent(sourceSrc).getField(columnSrc);
        if(f==null)return;
        if(meta==null)meta = newData.getContent(sourceSrc).getMetadata(columnSrc);
        if(fisrt) {
            value = f;
            fisrt = false;
        }else if(value.compareTo(f)>0){
            value = f;
        }
    }

}
