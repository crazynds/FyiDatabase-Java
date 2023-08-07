package sgbd.prototype.query.fields;

import sgbd.prototype.BData;
import sgbd.prototype.metadata.Metadata;

public class BooleanField extends Field<Boolean>{
    public BooleanField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    @Override
    protected Boolean constructData() {
        return data.getBoolean();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        if(!f.metadata.isBoolean())return NOT_DEFINED;
        return getBufferedData().compareTo(f.getBoolean());
    }
}
