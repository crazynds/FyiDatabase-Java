package sgbd.table;

import engine.file.FileManager;
import engine.virtualization.record.manager.FixedBTreeRecordManager;
import engine.virtualization.record.manager.FixedRecordManager;
import sgbd.prototype.Prototype;

public class BTreeDoubleTable extends DoubleTable{
    public BTreeDoubleTable(String tableName, Prototype pt) {
        super(tableName, pt);
    }

    @Override
    public void open() {
        if(index==null){
            index = new FixedBTreeRecordManager(new FileManager(tableName+"-index.dat"),indexTranslator,indexTranslator.getPrimaryKeySize(),maxSizeIndexRowData);
        }
        if(data==null){
            data = new FixedRecordManager(new FileManager(tableName+"-data.dat"),dataTranslator,maxSizeDataRowData);
        }
    }
}
