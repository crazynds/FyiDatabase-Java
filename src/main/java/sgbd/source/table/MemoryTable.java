package sgbd.source.table;

import engine.storage.common.MemoryHeapStorageRecord;

import sgbd.source.components.Header;

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
        if(storage==null)
            this.storage = new MemoryHeapStorageRecord((r, key) -> {});
    }
}
