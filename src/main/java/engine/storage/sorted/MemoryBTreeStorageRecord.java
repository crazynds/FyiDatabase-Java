package engine.storage.sorted;

import engine.exceptions.DataBaseException;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.instances.GenericRecordPK;
import lib.BigKey;
import lib.btree.BPlusTree;


import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MemoryBTreeStorageRecord extends PkStorageRecord<BigKey> {

    private BPlusTree<BigKey,byte[]> arvoreB;

    public MemoryBTreeStorageRecord() {
        arvoreB = new BPlusTree<>();
    }

    @Override
    public void restart() {
        arvoreB.clear();
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }

    @Override
    public RecordStream<BigKey> read(BigKey key) {
        return new RecordStream<BigKey>() {

            Iterator<Map.Entry<BigKey, byte[]>> iterator;

            Map.Entry<BigKey, byte[]> current = null;
            @Override
            public void open() {
                if(key==null)
                    iterator = arvoreB.iterator();
                else
                    iterator = arvoreB.iterator(key);
            }

            @Override
            public void close() {
                iterator = null;
            }

            @Override
            public BigKey getKey() {
                if(current!=null)return current.getKey();
                return null;
            }

            @Override
            public Record getRecord() {
                if(current!=null)return new GenericRecordPK(current.getKey(),current.getValue());
                return null;
            }

            @Override
            public void update(Record r) {
                throw new DataBaseException("MemoryBTreeStorageRecord->iterator->update","Essa operação não é suportada para essa estrutura.");
            }

            @Override
            public void seek(BigKey key) {
                this.iterator = arvoreB.iterator(key);
            }

            @Override
            public boolean hasNext() {
                if(iterator==null)return false;
                return iterator.hasNext();
            }

            @Override
            public Record next() {
                if(iterator==null)return null;
                current = iterator.next();
                return getRecord();
            }
        };
    }

    @Override
    public void write(BigKey key, Record r) {
        arvoreB.insert(key,r.getData());
    }

    @Override
    public void write(List<Map.Entry<BigKey, Record>> list) {
        for (Map.Entry<BigKey, Record> entry:
             list) {
            arvoreB.insert(entry.getKey(),entry.getValue().getData());
        }
    }

    @Override
    public void delete(BigKey key) {
        arvoreB.remove(key);
    }
}
