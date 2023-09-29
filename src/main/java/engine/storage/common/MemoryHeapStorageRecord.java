package engine.storage.common;

import engine.storage.common.AnonymousStorageRecord;
import engine.virtualization.interfaces.StorageEventHandler;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MemoryHeapStorageRecord extends AnonymousStorageRecord{

    private ArrayList<Record> list;


    public MemoryHeapStorageRecord(StorageEventHandler handler) {
        super(handler);
        list = new ArrayList<>();
    }

    @Override
    public void restart() {
        list.clear();
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public RecordStream read(Long key) {
        return new RecordStream() {
            Iterator<Record> it;
            long currentKey = 0;
            Record buffer;

            @Override
            public void open() {
                it = list.iterator();
                for(int x=0;x<key && hasNext();x++)next();
            }

            @Override
            public void close() {
                it = null;
                currentKey = -1;
            }

            @Override
            public Long getKey() {
                return currentKey;
            }

            @Override
            public Record getRecord() {
                return buffer;
            }

            @Override
            public void update(Record r) {
                list.set((int) currentKey,r);
            }

            @Override
            public void reset() {
                close();
                open();
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Record next() {
                buffer = it.next();
                currentKey += 1;
                return buffer;
            }
        };
    }

    @Override
    public synchronized void write(Record r) {
        list.add(r);
        handler.updateRecord(r,this.list.size()-1);
    }

    @Override
    public synchronized void write(List<Record> list) {
        for (Record r:
             list) {
            Integer position = this.list.size();
            this.list.add(r);
            handler.updateRecord(r,position);
        }
    }

    @Override
    public synchronized void update(long key, Record r) {
        list.set((int) key,r);
        handler.updateRecord(r,key);

    }

    @Override
    public synchronized void delete(long key) {
        if(key<list.size()-1) {
            Record r = list.get(list.size() - 1);
            list.set((int) key, r);
            handler.updateRecord(r,key);
        }
        list.remove(list.size()-1);
    }
}
