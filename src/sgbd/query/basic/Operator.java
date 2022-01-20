package sgbd.query.basic;

public interface Operator {

    public void open();
    public Tuple next();
    public boolean hasNext();
    public void close();
}
