package sgbd.table;

import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;

import java.math.BigInteger;
import java.util.Iterator;

public class SimpleTable extends Table {
    public SimpleTable(Prototype pt) {
        super(pt);
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    public void insert(RowData r) {

    }

    @Override
    public RowData find(BigInteger pk) {
        return null;
    }

    @Override
    public void update(RowData r) {

    }

    @Override
    public RowData delete(BigInteger pk) {
        return null;
    }

    @Override
    public Iterator<RowData> iterator() {
        return null;
    }
}
