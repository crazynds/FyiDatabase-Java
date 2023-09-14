package engine.virtualization.record.instances;

import java.math.BigInteger;

public class GenericRecordPK extends GenericRecord{

    private BigInteger pk;

    public GenericRecordPK(BigInteger pk, byte[] data) {
        this(pk,data,data.length);
    }

    public GenericRecordPK(BigInteger pk,byte[] data, int size) {
        super(data, size);
    }

    @Override
    public BigInteger primaryKey() {
        return this.pk;
    }
}
