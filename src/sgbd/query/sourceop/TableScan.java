package sgbd.query.sourceop;

import sgbd.prototype.ComplexRowData;
import sgbd.query.Tuple;
import sgbd.table.Table;
import sgbd.table.components.RowIterator;

import java.util.List;

public class TableScan extends SourceOperator {

    private List<String> columns;
    protected RowIterator iterator;

    public TableScan(Table t){
        super(t);
    }
    public TableScan(Table t,List<String> columns){
        super(t);
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
        ComplexRowData row = iterator.next();
        if(row==null)return null;
        Tuple tuple = new Tuple();
        tuple.setContent(sourceName(),row);
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
}
