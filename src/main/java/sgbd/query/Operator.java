package sgbd.query;

import lib.booleanexpression.entities.AttributeFilters;
import sgbd.prototype.query.Tuple;
import sgbd.source.Source;

import java.util.List;
import java.util.Map;

public interface Operator {
    
    public void lookup(AttributeFilters filters);

    public void open();
    public Tuple next();
    public boolean hasNext();
    public void close();
    public void freeResources();

    public List<Source> getSources();
    public Map<String,List<String>> getContentInfo();


}
