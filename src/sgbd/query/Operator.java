package sgbd.query;

import sgbd.table.Table;

import java.util.List;
import java.util.Map;

public interface Operator {

    public void open();
    public Tuple next();
    public boolean hasNext();
    public void close();

    public List<Table> getSources();
    public Map<String,List<String>> getContentInfo();


}
