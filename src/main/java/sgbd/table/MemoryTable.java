package sgbd.table;

import engine.virtualization.record.manager.MemoryBTreeRecordManager;

import sgbd.prototype.Prototype;
import sgbd.table.components.Header;

public class MemoryTable extends GenericTable{

    private int maxRecordSize;

    MemoryTable(Header header) {
        super(header);
        header.set(Header.TABLE_TYPE,"MemoryTable");
        header.setBool("clear",false);
        maxRecordSize = this.translatorApi.maxRecordSize();
    }

    @Override
    public void open() {
        if(manager==null)
            this.manager = new MemoryBTreeRecordManager(this.translatorApi);
    }
}
