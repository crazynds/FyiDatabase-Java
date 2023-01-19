package sgbd.table;

import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.virtualization.record.manager.FixedRecordManager;
import sgbd.prototype.Prototype;
import sgbd.table.components.Header;

public class SimpleTable extends GenericTable {

    private FileManager fm;

    SimpleTable(Header header) {
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
        if(manager==null)
            this.manager = new FixedRecordManager(this.fm,this.translatorApi,this.translatorApi.maxRecordSize());
    }

}
