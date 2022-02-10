package sgbd.query.basic.sourceop;

import sgbd.prototype.RowData;
import sgbd.query.basic.Tuple;
import sgbd.table.Table;

import java.math.BigInteger;
import java.util.List;

public class PKTableScan extends SourceOperator{

    private List<String> columns;
    private BigInteger pk;
    private RowData row;

    public PKTableScan(Table table, BigInteger pk) {
        super(table);
        this.pk=pk;
    }
    public PKTableScan(Table table, BigInteger pk, List<String> columns) {
        super(table);
        this.pk=pk;
        this.columns = columns;
    }

    @Override
    public void open() {
        try {
            if (columns != null)
                row = table.find(pk, columns);
            else row = table.find(pk);
        }catch (Exception e){
            row = null;
        }
    }

    @Override
    public Tuple next() {
        if(row==null)return null;
        Tuple t =new Tuple();
        t.setContent(sourceName(),row);
        row=null;
        return t;
    }

    @Override
    public boolean hasNext() {
        return row!=null;
    }

    @Override
    public void close() {
        row = null;
    }
}
