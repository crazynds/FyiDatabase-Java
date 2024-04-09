package engine.virtualization.record.instances;


import lib.BigKey;
import sgbd.prototype.RowData;

public class CompleteGenericRecord extends GenericRecord{

    private RowData row;

    public CompleteGenericRecord(RowData row, byte[] data) {
        this(row,data,data.length);
    }

    public CompleteGenericRecord(RowData row,byte[] data, int size) {
        super(data, size);
    }

    public RowData getRowData() {
        return this.row;
    }
}
