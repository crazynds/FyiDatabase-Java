package sgbd.table;

import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.virtualization.record.manager.FixedBTreeRecordManager;
import engine.virtualization.record.manager.FixedRecordManager;
import sgbd.prototype.Prototype;

public class BTreeTable extends GenericTable{
    private FileManager fm;

    public BTreeTable(String tableName,FileManager fm , Prototype pt) {
        super(tableName, pt);
        this.fm = fm;
    }
    public static Table openTable(String name,Prototype p){
        return BTreeTable.openTable(name,p,false);
    }
    public static Table openTable(String name,Prototype p, boolean clear){
        FileManager fm = new FileManager(name+".dat", new OptimizedFIFOBlockBuffer(4));
        if(clear)fm.clearFile();
        return new BTreeTable(name,fm,p);
    }

    @Override
    public void open() {
        if(manager==null)
            this.manager = new FixedBTreeRecordManager(this.fm,this.translatorApi,this.translatorApi.getPrimaryKeySize(),this.translatorApi.maxRecordSize());
    }
}
