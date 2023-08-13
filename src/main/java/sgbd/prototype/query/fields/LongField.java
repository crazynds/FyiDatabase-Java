package sgbd.prototype.query.fields;

import sgbd.prototype.BData;
import sgbd.prototype.metadata.FloatMetadata;
import sgbd.prototype.metadata.LongMetadata;
import sgbd.prototype.metadata.Metadata;

public class LongField extends Field<Long>{
    public LongField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    public LongField(long value) {
        super(LongMetadata.generic, value);
    }
    @Override
    protected Long constructData() {
        return data.getLong();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        if(f.metadata.isString() || f.metadata.isFloat())return f.compareTo(this);
        if(!f.metadata.isInt())return NOT_DEFINED;
        Long val;
        if(f.metadata.getSize()==8){
            val = f.getLong();
        } else {
            val = f.getInt().longValue();
        }
        return getBufferedData().compareTo(val);
    }
}
