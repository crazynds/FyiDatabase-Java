package sgbd.prototype.query.fields;

import lib.booleanexpression.entities.elements.Null;
import sgbd.prototype.metadata.IntegerMetadata;
import sgbd.prototype.metadata.LongMetadata;
import sgbd.prototype.metadata.Metadata;
import sgbd.prototype.metadata.StringMetadata;

public class NullField extends Field<Object>{
    public static final NullField generic = new NullField(IntegerMetadata.generic);

    public NullField(Metadata metadata) {
        super(metadata, null);
    }

    public NullField() {
        super(IntegerMetadata.generic, null);
    }
    @Override
    protected Object constructData() {
        return null;
    }

    @Override
    public int compareTo(Field f) {
        if(f==null)return 0;
        return NULL_COMPARE * -1;
    }
}
