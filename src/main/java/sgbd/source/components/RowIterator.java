package sgbd.source.components;

import sgbd.prototype.RowData;

import java.util.Iterator;

public interface RowIterator<T> extends Iterator<RowData> {

    public void restart();

    public void unlock();

    public T getRefKey();

}
