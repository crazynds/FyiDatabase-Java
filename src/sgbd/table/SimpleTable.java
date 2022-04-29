package sgbd.table;

import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.virtualization.record.manager.FixedRecordManager;
import sgbd.prototype.Prototype;

public class SimpleTable extends GenericTable {

    private FileManager fm;

    private int maxRecordSize;

    public SimpleTable(String tableName, FileManager fm, Prototype pt) {
        super(tableName,pt);
        this.fm = fm;
        maxRecordSize = this.translatorApi.maxRecordSize();
    }

    public static Table openTable(String name,Prototype p){
        return SimpleTable.openTable(name,p,false);
    }
    public static Table openTable(String name,Prototype p, boolean clear){
        FileManager fm = new FileManager(name+".dat", new OptimizedFIFOBlockBuffer(4));
        if(clear)fm.clearFile();
        return new SimpleTable(name,fm,p);
    }

    @Override
    public void open() {
        if(manager==null)
            this.manager = new FixedRecordManager(this.fm,this.translatorApi,this.translatorApi.maxRecordSize());
    }

}
