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
    public RowData findByRef(BigKey reference) {
        Iterator<Map.Entry<BigKey,T>> it = storage.iterator(reference);
        if(it.hasNext()){
            Map.Entry<BigKey,T> row = it.next();
            BigKey pk = row.getKey();
            if(nonUniqueIndex) {
                RowData rowdata = translatorApi.convertBinaryToRowData(row.getKey().getData(),null,false,true);
                rowdata.unset(UNIQUE_COLUMN_NAME);
                pk = translatorApi.getPrimaryKey(rowdata);
            }

            if(pk.compareTo(reference) == 0)
                return src.findByRef(row.getValue());
        }
        return null;
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
            public T getRefKey() {
                if(current == null) return null;
                return current.getValue();
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public RowData next() {
                current = it.next();
                T ref = current.getValue();
                RowData row = translatorApi.convertBinaryToRowData(current.getKey().getData(),metaInfo,false,true);
                if(nonUniqueIndex)
                    row.unset(UNIQUE_COLUMN_NAME);
//                if(ref instanceof Long){
//                    row.setLong("#ref",(Long)ref);
//                }else if(ref instanceof BigKey){
//                    row.setBigKey("#ref",(BigKey) ref);
//                }
                return row;
            }
        };
    }
}
