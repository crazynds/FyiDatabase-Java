package sgbd.query.sourceop;

import lib.BigKey;
import lib.booleanexpression.entities.AttributeFilters;
import lib.booleanexpression.entities.elements.Value;
import sgbd.info.Query;
import sgbd.prototype.BData;
import sgbd.prototype.RowData;
import sgbd.prototype.column.Column;
import sgbd.prototype.metadata.Metadata;
import sgbd.prototype.query.Tuple;
import sgbd.prototype.query.fields.BinaryField;
import sgbd.source.Source;
import sgbd.source.index.Index;
import sgbd.source.table.Table;
import sgbd.source.components.RowIterator;

import java.util.*;

public class SourceScan extends SourceOperator{

    protected List<String> columns;
    protected RowIterator iterator;

    // Lookup variables
    private RowData lowerbound = null;
    private BigKey upperbound = null;
    private boolean forceStop;

    public SourceScan(Source t){
        super(t);
        columns = t.getTranslator().getColumns().stream().map(Column::getName).toList();
    }
    public SourceScan(Table t,List<String> columns){
        super(t);
        this.columns=columns;
    }


    @Override
    public void lookup(AttributeFilters filters) {
        Source t = this.source;
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
        if(iterator==null) {
            if (lowerbound!=null) {
                iterator = source.iterator(columns,lowerbound);
            } else {
                iterator = source.iterator(columns);
            }
        }else{
            if (lowerbound!=null) {
                iterator.unlock();
                iterator = source.iterator(columns,lowerbound);
            }else
                iterator.restart();
        }
    }

    @Override
    public Tuple next() {
        RowData row = iterator.next();
        if(row==null)return null;
        if(upperbound!=null){
            BigKey current = source.getTranslator().getPrimaryKey(row);
            if(current.compareTo(upperbound) > 0)forceStop = true;
        }
        Tuple tuple = new Tuple();
        tuple.setContent(sourceName(),row);
        return tuple;
    }

    @Override
    public boolean hasNext() {
        if(forceStop) return true;
        return iterator.hasNext();
    }

    @Override
    public void close() {
        lowerbound = null;
        upperbound = null;
        forceStop = false;
    }

    @Override
    public void freeResources() {
        if(iterator!=null)
            iterator.unlock();
    }

    @Override
    public Map<String, List<String>> getContentInfo() {
        HashMap<String,List<String>> map = new LinkedHashMap<>();
        map.put(sourceName(),new ArrayList<>(columns));
        return map;
    }
}
