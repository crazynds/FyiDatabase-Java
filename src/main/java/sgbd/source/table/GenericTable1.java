package sgbd.source.table;

import engine.storage.sorted.BTreeRecordStream;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;
import ibd.index.btree.BPlusTreeFile1;
import ibd.index.btree.DictionaryPair;
import ibd.index.btree.Key;
import ibd.index.btree.RowSchema;
import ibd.index.btree.Value;
import ibd.persistent.PersistentPageFile;
import ibd.persistent.cache.Cache;
import java.nio.file.Paths;
import lib.BigKey;
import sgbd.prototype.column.Column;
import sgbd.prototype.RowData;
import sgbd.source.components.Header;
import sgbd.source.components.RowIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import sgbd.source.Source;

public class GenericTable1 extends Source<BigKey> {

    BPlusTreeFile1 tree = null;

    public GenericTable1(Header header, String folder, String name, int pageSize, boolean override) throws Exception {
        super(header);

        RowSchema keyPrototype = new RowSchema(1);
        keyPrototype.addBigKeyDataType(this.getTranslator().getPrimaryKeySize());

        RowSchema valuePrototype = new RowSchema(1);
        valuePrototype.addRecordDataType(this.getTranslator().maxRecordSize());

        //defines the paged file that the BTree will use
        PersistentPageFile p = new PersistentPageFile(pageSize, Paths.get(folder + "\\" + name), override);
        //LRUCache lru = new LRUCache(5000000, p);

        Cache lru = null;
        //defines the buffer management to be used, if any.
        lru = new ibd.persistent.cache.LRUCache(5000000, p);
        //lru = new MidPointCache(5000000, p);

        //creates a BTree instance using the defined buffer manager, if any
        if (lru != null) {
            tree = new BPlusTreeFile1(5, 7, lru, keyPrototype, valuePrototype);
        } else {
            tree = new BPlusTreeFile1(5, 7, p, keyPrototype, valuePrototype);
        }
    }

    @Override
    public void close() {
        tree.flush();
        tree.close();
    }

    @Override
    public void clear() {
        //clearing the btree is to be implemented yet
    }

    public BigKey insert(RowData r) {
        translatorApi.validateRowData(r);
        BigKey bigKey = translatorApi.getPrimaryKey(r);
        Record record = translatorApi.convertToRecord(r);

        Key key = new Key(tree.getKeySchema());
        key.setKeys(new BigKey[]{bigKey});

        Value value = new Value(tree.getValueSchema());

        value.set(0, record);

        boolean ok = tree.insert(key, value);
        if (!ok) {
            return null;
        }
        //this.tree.flush();
        return bigKey;
    }

    public void insert(List<RowData> r) {
        for (RowData row : r) {
            this.insert(row);
        }
        //this.tree.flush();
    }

    public BigKey update(RowData r) {
        translatorApi.validateRowData(r);
        BigKey bigKey = translatorApi.getPrimaryKey(r);
        Record record = translatorApi.convertToRecord(r);

        Key key = new Key(tree.getKeySchema());
        key.setKeys(new BigKey[]{bigKey});

        Value value = new Value(tree.getValueSchema());

        value.set(0, record);

        Value v = tree.update(key, value);
        if (v == null) {
            return null;
        }

        //this.tree.flush();
        return bigKey;
    }

    public BigKey delete(RowData r) {
        translatorApi.validateRowData(r);
        BigKey bigKey = translatorApi.getPrimaryKey(r);

        Key key = new Key(tree.getKeySchema());
        key.setKeys(new BigKey[]{bigKey});

        Value value = tree.delete(key);
        if (value == null) {
            return null;
        }

        //this.tree.flush();
        return bigKey;
    }

    @Override
    public RowIterator iterator(List<String> columns) {
        return this.iterator(columns, null);
    }

    @Override
    public RowIterator iterator() {
        ArrayList<String> columns = new ArrayList<>();
        for (Column c : this.getTranslator()) {
            columns.add(c.getName());
        }
        return this.iterator(columns);
    }

    @Override
    protected RowIterator iterator(List<String> columns, BigKey lowerBound) {
        return new RowIterator() {
            boolean started = false;
            RecordStream<BigKey> recordStream;
            Map<String, Column> metaInfo = translatorApi.generateMetaInfo(columns);

            private void start() {
                ArrayList<DictionaryPair> pairs = null;
                if (lowerBound == null) {
                    pairs = tree.searchAll();
                } else {
                    Key key = new Key(tree.getKeySchema());
                    key.setKeys(new BigKey[]{lowerBound});

                    pairs = tree.partialSearchDP(key);
                }

                recordStream = new BTreeRecordStream(pairs);
                recordStream.open();
                started = true;
            }

            @Override
            public void restart() {
                if (!started || recordStream == null) {
                    start();
                }
                recordStream.reset();
            }

            @Override
            public boolean hasNext() {
                if (!started) {
                    start();
                }
                if (recordStream == null) {
                    return false;
                }
                boolean val = recordStream.hasNext();
                if (!val) {
                    unlock();
                }
                return val;
            }

            @Override
            public RowData next() {
                if (!started) {
                    start();
                }
                if (recordStream == null) {
                    return null;
                }
                Record record = recordStream.next();
                if (record == null) {
                    unlock();
                    return null;
                }
                return translatorApi.convertToRowData(record, metaInfo);
            }

            @Override
            public void unlock() {
                if (recordStream == null) {
                    return;
                }
                recordStream.close();
                recordStream = null;
            }

            @Override
            public BigKey getRefKey() {
                return recordStream.getKey();
            }
        };
    }

    @Override
    public void open() {
        // a abertura é implícita
    }

    @Override
    public RowData findByRef(BigKey reference) {
        Key key = new Key(tree.getKeySchema());
        key.setKeys(new BigKey[]{reference});

        Value v = tree.search(key);
        if(v==null)return null;
        Record record = (Record) v.get(0);
        RowData rowData = this.translatorApi.convertToRowData(record, null);
        return rowData;

    }

    @Override
    public String getSourceName() {
        return "alguma coisa";
    }

}
