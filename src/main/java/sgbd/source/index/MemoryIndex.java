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

public class MemoryIndex<T> extends Index<T>{


    //TODO: usar a btree da engina ao invez da BTree da lib
    //private MemoryBTreeStorageRecord storage;
    private BPlusTree<BigKey,T> storage;

    public MemoryIndex(Header header, Source<T> src) {
        super(header,src);
        storage = new BPlusTree<>();
        reindex();
    }

    @Override
    protected void updateRef(BigKey key, T reference) {
        storage.insert(key,reference);
    }

    @Override
    public T deleteRef(BigKey key) {
        // TODO: Fazer ele remover a entrada com a mesma referencia a partir da key dada
        if(storage.get(key)!=null)
            return storage.remove(key);
        return null;
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
    protected RowIterator iterator(List<String> columns, BigKey lowerbound) {
        return new RowIterator() {

            Iterator<Map.Entry<BigKey,T>> it = lowerbound!=null ? storage.iterator(lowerbound) : storage.iterator();
            Map.Entry<BigKey,T> current = null;
            Map<String, Column> metaInfo = translatorApi.generateMetaInfo(columns);

            @Override
            public void restart() {
                it = storage.iterator(lowerbound);
            }

            @Override
            public void unlock() {
            }

            @Override
            public BigKey getRefKey() {
                if(current == null) return null;
                return current.getKey();
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public RowData next() {
                Map.Entry<BigKey,T> entry = it.next();
                T ref = entry.getValue();
                RowData row = translatorApi.convertBinaryToRowData(entry.getKey().getData(),metaInfo,false,true);

                return row;
            }
        };
    }
}
