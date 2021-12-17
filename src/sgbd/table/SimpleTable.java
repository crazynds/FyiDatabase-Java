package sgbd.table;

import sgbd.prototype.Prototype;
import sgbd.prototype.RowData;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;

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
    public BigInteger insert(RowData r) {
        translatorApi.validateRowData(r);
        BigInteger pk = translatorApi.getPrimaryKey(r);

        return pk;
    }

    @Override
    public RowData find(BigInteger pk, List<String> colunas) {
        return null;
    }

    @Override
    public RowData update(BigInteger pk,RowData r) {
        return null;
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
