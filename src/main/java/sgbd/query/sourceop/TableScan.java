package sgbd.query.sourceop;

import com.mysql.cj.result.Row;
import lib.BigKey;
import lib.booleanexpression.entities.AttributeFilters;
import lib.booleanexpression.entities.elements.Value;
import sgbd.info.Query;
import sgbd.prototype.BData;
import sgbd.prototype.RowData;
import sgbd.prototype.column.Column;
import sgbd.prototype.query.Tuple;
import sgbd.prototype.query.fields.BinaryField;
import sgbd.source.table.Table;
import sgbd.source.components.RowIterator;

import java.util.*;

public class TableScan extends SourceScan {

    private RowData lowerbound = null;
    private BigKey upperbound = null;

    public TableScan(Table t){
        super(t);
    }
    public TableScan(Table t,List<String> columns){
        super(t,columns);
    }

    @Override
    public void lookup(AttributeFilters filters) {
        Table t = (Table)this.source;
        RowData upperbound = null;
        lowerbound=null;
        for(Column c:t.getTranslator()){
            Map.Entry <Value, Value> entry = filters.getColumnFilter(sourceName()+'.'+c.getName());
            if(c.isPrimaryKey() && entry!=null && entry.getKey()!=null){
                if(lowerbound==null) {
                    lowerbound = new RowData();
                    upperbound = new RowData();
                }
                lowerbound.setField(c.getName(),entry.getKey().getField());
                if(entry.getValue()==null){
                    byte[] arr = new byte[c.getSize()];
                    Arrays.fill(arr, (byte)-1);
                    upperbound.setField(c.getName(), BinaryField.createField(c, new BData(arr)));
                }else upperbound.setField(c.getName(),entry.getValue().getField());
            }else{
                break;
            }
        }
        if(lowerbound!=null){
            Query.LOOK_UP_APPLIED +=1;
            this.upperbound = t.getTranslator().getPrimaryKey(upperbound);
        }
    }

    @Override
    public void open() {
        if(lowerbound!=null){
            if(iterator!=null){
                iterator.unlock();
                iterator = null;
            }
            iterator = source.iterator(columns,lowerbound);
        }else{
            super.open();
        }
    }


    @Override
    public void close() {
        lowerbound = null;
        super.close();
    }
}
