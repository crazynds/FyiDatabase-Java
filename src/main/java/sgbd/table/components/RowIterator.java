package sgbd.table.components;

import sgbd.prototype.RowData;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;

public interface RowIterator extends Iterator<RowData> {

    public void setPointerPk(BigInteger pk);
    public void restart();

    public void unlock();

    public Map.Entry<BigInteger, RowData> nextWithPk();

}
