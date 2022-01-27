package sgbd.query.basic.sourceop;

import sgbd.query.basic.Tuple;
import sgbd.table.Table;

public class PKTableScan extends SourceOperator{

    public PKTableScan(Table table) {
        super(table);
    }

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
}
