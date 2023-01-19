package sgbd.table;

import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.virtualization.record.manager.FixedBTreeRecordManager;
import engine.virtualization.record.manager.FixedRecordManager;
import sgbd.prototype.Prototype;
import sgbd.table.components.Header;

public class BTreeTable extends GenericTable{
    private FileManager fm;

    BTreeTable(Header header) {
        super(header);
        this.fm = new FileManager(header.getTablePath(), new OptimizedFIFOBlockBuffer(4));
        if(header.getBool("clear")){
            fm.clearFile();
            header.setBool("clear",false);
        }
        header.set(Header.TABLE_TYPE,"DoubleTable");
        header.set(Header.FILE_PATH,fm.getFile().getPath());
    }

    @Override
    public void open() {
        if(manager==null)
            this.manager = new FixedBTreeRecordManager(this.fm,this.translatorApi,this.translatorApi.getPrimaryKeySize(),this.translatorApi.maxRecordSize());
    }
}
