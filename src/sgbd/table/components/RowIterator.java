package sgbd.table.components;

import sgbd.prototype.ComplexRowData;
import sgbd.prototype.RowData;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;

public interface RowIterator extends Iterator<ComplexRowData> {

    public void setPointerPk(BigInteger pk);
    public void restart();

    public Map.Entry<BigInteger, ComplexRowData> nextWithPk();

}
