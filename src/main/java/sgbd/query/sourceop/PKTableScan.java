package sgbd.query.sourceop;

import sgbd.info.Query;
import sgbd.prototype.Column;
import sgbd.prototype.ComplexRowData;
import sgbd.query.Tuple;
import sgbd.table.Table;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PKTableScan extends SourceOperator{

    private List<String> columns;
    private BigInteger pk;
    private ComplexRowData row;

    public PKTableScan(Table table, BigInteger pk) {
        super(table);
        this.pk=pk;
        columns = new ArrayList<String>();
        for(Column c : table.getTranslator()){
            columns.add(c.getName());
        }
    }
    public PKTableScan(Table table, BigInteger pk, List<String> columns) {
        super(table);
        this.pk=pk;
        this.columns = columns;
    }

    public void setPrimaryKey(BigInteger pk){
        this.pk = pk;
    }

    @Override
    public void open() {
        try {
            Query.PK_SEARCH++;
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

    @Override
    public Map<String, List<String>> getContentInfo() {
        HashMap<String,List<String>> map = new HashMap<>();
        map.put(sourceName(),columns);
        return map;
    }
}