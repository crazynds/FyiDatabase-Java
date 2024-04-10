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
import sgbd.prototype.query.fields.Field;
import sgbd.prototype.query.fields.NullField;
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
    private List<Map.Entry<String, Field>> upperbound = null;

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
        if(!Query.LOOK_UP_ENABLED)return;
        Source t = this.source;
        lowerbound=null;
        for(Column c:t.getTranslator()){
            Map.Entry <Field, Field> entry = filters.getColumnFilter(sourceName()+'.'+c.getName());
            if(c.isPrimaryKey() && entry!=null && entry.getKey()!=null){
                if(lowerbound==null) {
                    lowerbound = new RowData();
                    upperbound = new ArrayList<>();
                }
                if(entry.getValue() !=null && lowerbound.size() == upperbound.size())
                    upperbound.add(Map.entry(c.getName(), entry.getValue()));
                lowerbound.setField(c.getName(),entry.getKey());
            }else{
                break;
            }
        }
        if(lowerbound!=null){
            Query.LOOK_UP_LOWERBOUND +=1;
        }
    }

    @Override
    public void open() {
        if (lowerbound!=null) {
            freeResources();
            iterator = source.iterator(columns,lowerbound);
        } else {

            if(upperbound==null)Query.FULL_TABLE_SCAN += 1;

            if(iterator==null)
                iterator = source.iterator(columns);
            else iterator.restart();
        }
    }

    @Override
    public Tuple next() {
        RowData row = iterator.next();
        if(row==null)return null;
        if(upperbound!=null && !upperbound.isEmpty()){
            int status = 0;
            for(Map.Entry<String,Field> field:upperbound){
                status = row.getField(field.getKey()).compareTo(field.getValue());
                if(status!=0)break;
            }
            if(status > 0){
                forceStop = true;
                Query.LOOK_UP_UPPERBOUND += 1;
            }
        }
        Tuple tuple = new Tuple();
        tuple.setContent(sourceName(),row);
        return tuple;
    }

    @Override
    public boolean hasNext() {
        if(forceStop) return false;
        return iterator.hasNext();
    }

    @Override
    public void close() {
        if(lowerbound!=null){
            freeResources();
            iterator = null;
        }
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
