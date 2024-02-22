package sgbd.source.index;

import lib.BigKey;
import lib.btree.BPlusTree;
import sgbd.prototype.RowData;
import sgbd.prototype.column.Column;
import sgbd.source.Source;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;


import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MemoryIndex extends Index{


    //TODO: usar a btree da engina ao invez da BTree da lib
    //private MemoryBTreeStorageRecord storage;
    private BPlusTree<BigKey,BigKey> storage;

    public MemoryIndex(Header header, Source src) {
        super(preparePrototype(header,src),src);
        storage = new BPlusTree<>();
    }

    @Override
    protected void insertPair(BigKey key, BigKey reference) {
        storage.insert(key,reference);
    }

    @Override
    public void clear() {
        storage.clear();
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    protected RowIterator idxIterator(List<String> columns, BigKey lowerbound) {
        return new RowIterator() {
            Iterator<Map.Entry<BigKey,BigKey>> it = lowerbound==null ? storage.iterator() : storage.iterator(lowerbound);

            @Override
            public void restart() {
                it = storage.iterator(lowerbound);
            }

            @Override
            public void unlock() {
                it = null;
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public RowData next() {
                Map.Entry<BigKey,BigKey> entry = it.next();
                if(entry==null)return null;
                return generateRowData(entry.getKey(),entry.getValue());
            }
        };
    }


}
