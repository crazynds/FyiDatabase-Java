package engine.storage.sorted;

import engine.exceptions.DataBaseException;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import lib.btree.BPlusTree;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MemoryBTreeStorageRecord extends PkStorageRecord<BigInteger> {

    private BPlusTree<BigInteger,byte[]> arvoreB;

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
    public RecordStream<BigInteger> read(BigInteger key) {
        return new RecordStream<BigInteger>() {

            Iterator<Map.Entry<BigInteger, byte[]>> iterator;

            Map.Entry<BigInteger, byte[]> current = null;
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
            public BigInteger getKey() {
                if(current!=null)return current.getKey();
                return null;
            }

            @Override
            public Record getRecord() {
                if(current!=null)return new GenericRecord(current.getValue());
                return null;
            }

            @Override
            public void update(Record r) {
                throw new DataBaseException("MemoryBTreeStorageRecord->iterator->update","Essa operação não é suportada para essa estrutura.");
            }

            @Override
            public void reset() {
                close();
                open();
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
    public void write(BigInteger key, Record r) {
        arvoreB.insert(key,r.getData());
    }

    @Override
    public void write(List<Map.Entry<BigInteger, Record>> list) {
        for (Map.Entry<BigInteger, Record> entry:
             list) {
            arvoreB.insert(entry.getKey(),entry.getValue().getData());
        }
    }

    @Override
    public void delete(BigInteger key) {
        arvoreB.remove(key);
    }
}
