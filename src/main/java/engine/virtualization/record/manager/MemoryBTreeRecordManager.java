package engine.virtualization.record.manager;

import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import lib.btree.BPlusTree;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MemoryBTreeRecordManager extends RecordManager{

    private BPlusTree<BigInteger,byte[]> arvoreB;

    public MemoryBTreeRecordManager(RecordInfoExtractor ri) {
        super(null, ri);
        arvoreB = new BPlusTree<>();
    }

    @Override
    public void restart() {
        arvoreB.clear();
    }

    @Override
    public Record read(BigInteger pk) {
        byte[] data = arvoreB.get(pk);
        if(data==null)return null;
        Record r = new GenericRecord(data);
        return r;
    }

    @Override
    public void read(BigInteger pk, byte[] buffer) {
        byte[] data = arvoreB.get(pk);
        if(data==null)return;
        System.arraycopy(data,0,buffer,0,(data.length>buffer.length)?buffer.length:data.length);
    }

    @Override
    public void write(Record r) {
        BigInteger pk = recordInterface.getPrimaryKey(r);
        arvoreB.insert(pk,r.getData().clone());
    }

    @Override
    public void write(List<Record> list) {
        for(Record r:list){
            write(r);
        }
    }

    @Override
    public boolean isOrdened() {
        return true;
    }

    @Override
    public RecordStream sequencialRead() {
        RecordManager self = this;

        return new RecordStream() {


            Iterator<Map.Entry<BigInteger,byte[]>> iterator;

            Map.Entry<BigInteger,byte[]> entry;
            BigInteger pointer = null;
            GenericRecord record = new GenericRecord(null);

            @Override
            public void open(boolean lockToWrite) {
                if(pointer!=null)
                    iterator = arvoreB.iterator(pointer);
                else
                    iterator = arvoreB.iterator();
            }

            @Override
            public void close() {
                iterator = null;
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Record next() {
                entry = iterator.next();
                record.setData(entry.getValue());
                return record;
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public void write(Record r) {
                arvoreB.insert(self.getRecordInterface().getPrimaryKey(r),r.getData());
            }

            @Override
            public void remove() {
                arvoreB.remove(entry.getKey());
            }


            @Override
            public void reset() {
                iterator = null;
                iterator = arvoreB.iterator();
            }

            @Override
            public void setPointer(BigInteger pk) {
                pointer = pk;
            }

            @Override
            public BigInteger getPointer() {
                return entry.getKey();
            }
        };
    }
}
