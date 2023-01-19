package sgbd.table;

import engine.file.FileManager;
import engine.virtualization.record.manager.FixedBTreeRecordManager;
import engine.virtualization.record.manager.FixedRecordManager;
import sgbd.prototype.Prototype;
import sgbd.table.components.Header;

public class BTreeDoubleTable extends DoubleTable{
    public BTreeDoubleTable(Header header) {
        super(header);
        header.set(Header.TABLE_TYPE,"BTreeDoubleTable");
    }

    @Override
    public void open() {
        if(index==null){
            index = new FixedBTreeRecordManager(indexFile,indexTranslator,indexTranslator.getPrimaryKeySize(),maxSizeIndexRowData);
        }
        if(data==null){
            data = new FixedRecordManager(dataFile,dataTranslator,maxSizeDataRowData);
        }
    }
}
