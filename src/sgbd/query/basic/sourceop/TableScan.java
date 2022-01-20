package sgbd.query.basic.sourceop;

import sgbd.prototype.RowData;
import sgbd.query.basic.Tuple;
import sgbd.table.Table;

import java.util.Iterator;
import java.util.List;

public class TableScan extends SourceOperator {

    private Table table;
    private List<String> columns;
    private Iterator<RowData> iterator;
    private String asName;

    public TableScan(Table t){
        table = t;
        asName = t.getTableName();
    }
    public TableScan(Table t,List<String> columns){
        table = t;
        asName = t.getTableName();
        this.columns=columns;
    }

    @Override
    public void open() {
        if(iterator==null)
            if(columns!=null) {
                iterator = table.iterator(columns);
            }else{
                iterator = table.iterator();
            }

    }

    @Override
    public Tuple next() {
        RowData row = iterator.next();
        if(row==null)return null;
        Tuple tuple = new Tuple();
        tuple.setContent(asName,row);
        return tuple;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public void close() {
        iterator=null;
    }

    @Override
    public void asName(String name) {
        this.asName=name;
    }

    @Override
    public String sourceName() {
        return this.asName;
    }
}
