package sgbd.query.sourceop;

import sgbd.prototype.RowData;
import sgbd.prototype.column.Column;
import sgbd.prototype.query.Tuple;
import sgbd.source.table.Table;
import sgbd.source.components.RowIterator;

import java.util.*;

public class TableScan extends SourceScan {

    public TableScan(Table t){
        super(t);
    }
    public TableScan(Table t,List<String> columns){
        super(t,columns);
    }


}
