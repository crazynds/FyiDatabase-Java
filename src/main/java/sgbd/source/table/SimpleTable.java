package sgbd.source.table;

import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.storage.common.FixedHeapStorageRecord;
import sgbd.prototype.RowData;
import sgbd.source.components.Header;
import sgbd.source.index.MemoryIndex;

public class SimpleTable extends GenericTable {

    private FileManager fm;

    public SimpleTable(Header header) {
        super(header);
        this.fm = new FileManager(header.getTablePath(), new OptimizedFIFOBlockBuffer(4));
        if(header.getBool("clear")){
            fm.clearFile();
            header.setBool("clear",false);
        }
        header.set(Header.FILE_PATH,fm.getFile().getPath());
        header.set(Header.TABLE_TYPE,"SimpleTable");
    }

    @Override
    public void open() {
        if(storage==null) {
            this.primaryIndex = new MemoryIndex(header.getSubHeader("primary_index"),this);
            this.storage = new FixedHeapStorageRecord((r, key) -> {
                RowData row = this.translatorApi.convertBinaryToRowData(r.getData(),null,true, true);
                this.primaryIndex.update(row,key);
            }, this.fm, this.translatorApi.maxRecordSize());
            this.primaryIndex.open();
            this.primaryIndex.reindex();
        }
    }

}
