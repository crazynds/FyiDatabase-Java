package sgbd.query.basic.sourceop;

import sgbd.query.basic.Tuple;

public class PKTableScan extends SourceOperator{
    @Override
    public void open() {

    }

    @Override
    public Tuple next() {

        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void asName(String name) {

    }

    @Override
    public String sourceName() {
        return null;
    }
}
