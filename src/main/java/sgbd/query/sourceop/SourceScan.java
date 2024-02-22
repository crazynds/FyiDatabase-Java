package sgbd.query.sourceop;

import sgbd.prototype.RowData;
import sgbd.prototype.column.Column;
import sgbd.prototype.metadata.Metadata;
import sgbd.prototype.query.Tuple;
import sgbd.source.Source;
import sgbd.source.index.Index;
import sgbd.source.table.Table;
import sgbd.source.components.RowIterator;

import java.util.*;

public class SourceScan extends SourceOperator{

    private List<String> columns;
    protected RowIterator iterator;

    public SourceScan(Source t){
        super(t);
        columns = new ArrayList<String>();

        for(Column c : t.getTranslator()){
            if(c.ignore())continue;
            columns.add(c.getName());
        }
    }
    public SourceScan(Table t,List<String> columns){
        super(t);
        this.columns=columns;
    }

    @Override
    public void open() {
        if(iterator==null) {
            if (columns != null) {
                iterator = source.iterator(columns);
            } else {
                iterator = source.iterator();
            }
        }else{
            iterator.restart();
        }

    }

    @Override
    public Tuple next() {
        RowData row = iterator.next();
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
    }

    @Override
    public void freeResources() {
        iterator.unlock();
    }

    @Override
    public Map<String, List<String>> getContentInfo() {
        HashMap<String,List<String>> map = new LinkedHashMap<>();
        map.put(sourceName(),new ArrayList<>(columns));
        return map;
    }
}
