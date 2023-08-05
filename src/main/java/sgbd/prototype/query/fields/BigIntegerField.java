package sgbd.prototype.query.fields;

import sgbd.prototype.BData;
import sgbd.prototype.metadata.Metadata;

import java.math.BigInteger;

public class BigIntegerField extends Field<BigInteger> {
    protected BigIntegerField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    @Override
    protected BigInteger constructData() {
        return data.getBigInteger();
    }

    @Override
    public int compareTo(Field f) {
        if(!f.metadata.isInt())return NOT_DEFINED;
        // TODO
        return 0;
    }
}
