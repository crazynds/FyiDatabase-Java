package sgbd.source.table;

import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.storage.common.FixedHeapStorageRecord;
import engine.virtualization.record.instances.CompleteGenericRecord;
import sgbd.prototype.RowData;
import sgbd.source.components.Header;
import sgbd.source.index.Index;
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
            Header idxHeader = header.getSubHeader("primary_index");
            idxHeader.setBool("primary_index",true);
            this.primaryIndex = new MemoryIndex(idxHeader,this);
            this.storage = new FixedHeapStorageRecord((r, key) -> {
                RowData row;
                if(r instanceof CompleteGenericRecord record){
                    row = record.getRowData();
                }else{
                    row = this.translatorApi.convertBinaryToRowData(r.getData(),null,true, true);
                }
                row.setLong(Index.REFERENCE_COLUMN_NAME,key);
                this.primaryIndex.insert(row);
            }, this.fm, this.translatorApi.maxRecordSize());
            this.primaryIndex.open();
            this.primaryIndex.reindex();
        }
    }

}
