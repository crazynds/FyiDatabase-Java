package sgbd.prototype.query.fields;

import sgbd.prototype.BData;
import sgbd.prototype.metadata.Metadata;

public class DoubleField extends Field<Double>{
    public DoubleField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    @Override
    protected Double constructData() {
        return data.getDouble();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        if(f.metadata.isString())return f.compareTo(this);
        if(!f.metadata.isFloat() && !f.metadata.isInt())return NOT_DEFINED;
        Double val;
        if(f.metadata.isInt()){
            if(f.metadata.getSize()==8){
                val = f.getLong().doubleValue();
            } else {
                val = f.getInt().doubleValue();
            }
        }else{
            if(f.metadata.getSize() == 4)
                val = f.getFloat().doubleValue();
            else
                val = f.getDouble();
        }
        return getBufferedData().compareTo(val);
    }
}
