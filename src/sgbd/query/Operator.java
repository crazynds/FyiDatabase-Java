package sgbd.query;

import sgbd.table.Table;

import java.util.List;

public interface Operator {

    public void open();
    public Tuple next();
    public boolean hasNext();
    public void close();

    public List<Table> getSources();



}
