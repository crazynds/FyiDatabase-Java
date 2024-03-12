package sgbd.source.components;

import lib.BigKey;
import sgbd.prototype.RowData;

import java.util.Iterator;

public interface RowIterator extends Iterator<RowData> {

    public void restart();

    public void unlock();

}
