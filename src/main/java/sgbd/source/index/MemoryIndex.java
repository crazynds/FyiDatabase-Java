package sgbd.source.index;

import lib.btree.BPlusTree;
import sgbd.prototype.RowData;
import sgbd.source.Source;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MemoryIndex<T> extends Index<T>{


    //TODO: usar a btree da engina ao invez da BTree da lib
    //private MemoryBTreeStorageRecord storage;
    private BPlusTree<BigInteger,T> storage;

    public MemoryIndex(Header header, Source<T> src) {
        super(header,src);
        storage = new BPlusTree<>();
        reindex();
    }

    @Override
    public void updateRef(BigInteger key, T reference) {
        storage.insert(key,reference);
    }

    @Override
    public void deleteRef(BigInteger key) {
        // TODO: Fazer ele remover a entrada com a mesma referencia a partir da key dada
//        Iterator<Map.Entry<BigInteger,Long>> it = storage.iterator(key);
//        while(it.hasNext()){
//            it.remove();
//        }
        if(storage.get(key)!=null)
            storage.remove(key);
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
    protected RowIterator iterator(List<String> columns, BigInteger lowerbound) {
        return new RowIterator() {

            Iterator<Map.Entry<BigInteger,T>> it = lowerbound!=null ? storage.iterator(lowerbound) : storage.iterator();
            Map.Entry<BigInteger,T> current = null;

            @Override
            public void restart() {
                it = storage.iterator(lowerbound);
            }

            @Override
            public void unlock() {
            }

            @Override
            public BigInteger getRefKey() {
                if(current == null) return null;
                return current.getKey();
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public RowData next() {
                T ref = it.next().getValue();
                RowData row = src.findByRef(ref);
                return row;
            }
        };
    }
}
