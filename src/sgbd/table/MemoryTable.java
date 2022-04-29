package sgbd.table;

import engine.virtualization.record.manager.MemoryBTreeRecordManager;

import sgbd.prototype.Prototype;

public class MemoryTable extends GenericTable{

    private int maxRecordSize;

    public MemoryTable(String tableName, Prototype pt) {
        super(tableName,pt);
        maxRecordSize = this.translatorApi.maxRecordSize();
    }

    @Override
    public void open() {
        if(manager==null)
            this.manager = new MemoryBTreeRecordManager(this.translatorApi);
    }
}
