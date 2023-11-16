package engine.virtualization.record.instances;


import lib.BigKey;

public class GenericRecordPK extends GenericRecord{

    private BigKey pk;

    public GenericRecordPK(BigKey pk, byte[] data) {
        this(pk,data,data.length);
    }

    public GenericRecordPK(BigKey pk,byte[] data, int size) {
        super(data, size);
    }

    public void setPrimaryKey(BigKey pk){
        this.pk = pk;
    }

    @Override
    public BigKey primaryKey() {
        return this.pk;
    }
}
