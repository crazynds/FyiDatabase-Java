package engine.storage.common;

import engine.exceptions.DataBaseException;
import engine.file.FileManager;
import engine.util.Util;
import engine.virtualization.interfaces.HeapStorage;
import engine.virtualization.interfaces.StorageEventHandler;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import lib.BigKey;

import java.io.IOException;

import java.util.List;

public class FixedHeapStorageRecord extends AnonymousStorageRecord{

    /*
     * Tamanho do inteiro que ira representar quantos records estão armazenados
     */
    protected final byte sizeOfBytesQtdRecords = 8;
    protected final int sizeOfEachRecord;
    protected final StorageEventHandler handler;

    protected HeapStorage heap;
    protected long qtdOfRecords = 0;

    public FixedHeapStorageRecord(StorageEventHandler handler,FileManager fm, int sizeOfEachRecord){
        super(handler);
        try {
            heap = new HeapStorage(fm);
            this.sizeOfEachRecord = sizeOfEachRecord;
            this.handler = handler;

            try {
                byte[] num = new byte[sizeOfBytesQtdRecords];
                heap.read(0, num, 0, sizeOfBytesQtdRecords);
                this.qtdOfRecords = Util.convertByteArrayToNumber(num).longValue();
            }catch(DataBaseException e){}

        }catch(IOException e){
            throw new DataBaseException("FixedRecordStorage->Constructor","Erro ao criar heap storage. "+e.getMessage());
        }
    }

    private long keyToPosition(long key){
        return 8+key*sizeOfEachRecord;
    }

    private long positionToKey(long position){
        long pos = (position-sizeOfBytesQtdRecords)/sizeOfEachRecord;
        if((position-sizeOfBytesQtdRecords)%sizeOfEachRecord!=0){
            pos += 1;
        }
        return pos;
    }

    @Override
    public synchronized void restart() {
        heap.clearFile();
        qtdOfRecords = 0;
    }

    @Override
    public synchronized void flush() {
        byte[] num = BigKey.valueOf(qtdOfRecords,sizeOfBytesQtdRecords).getData();
        heap.write(0,num,0,sizeOfBytesQtdRecords);
        heap.commitWrites();
    }


    @Override
    public RecordStream read(Long key) {
        return new RecordStream() {
            long currentKey;
            Record buffer = new GenericRecord(new byte[sizeOfEachRecord + 8]);

            @Override
            public void open() {
                currentKey = key;
            }

            @Override
            public void close() {
                currentKey = -1;
            }

            @Override
            public Long getKey() {
                if(currentKey <= 0) return (long) -1;
                return currentKey - 1;
            }

            @Override
            public Record getRecord() {
                if(currentKey == -1) return null;
                return buffer;
            }

            @Override
            public void update(Record r) {
                if(currentKey <= 0) return;
                heap.write(keyToPosition(currentKey - 1),r.getData(),r.size());
                handler.updateRecord(r,currentKey - 1);
            }

            @Override
            public void reset() {
                currentKey = key;
            }

            @Override
            public boolean hasNext() {
                if(currentKey == -1) return false;
                return currentKey < qtdOfRecords;
            }

            @Override
            public Record next() {
                if(!hasNext()){
                    currentKey = -1;
                    return null;
                }
                long position = keyToPosition(currentKey);
                heap.read(position,buffer.getData(),0,sizeOfEachRecord);
                currentKey += 1;
                return buffer;
            }
        };
    }

    @Override
    public synchronized void write(Record r) {
        long newPosition = keyToPosition(qtdOfRecords);
        heap.write(newPosition,r.getData(),r.size());
        this.handler.updateRecord(r,positionToKey(newPosition));
        qtdOfRecords+=1;
        heap.commitWrites();
    }

    @Override
    public synchronized void write(List<Record> list) {
        for (Record r:
             list) {
            long newPosition = keyToPosition(qtdOfRecords);
            heap.write(newPosition,r.getData(),r.size());
            this.handler.updateRecord(r,positionToKey(newPosition));
            qtdOfRecords+=1;
        }
        heap.commitWrites();
    }

    @Override
    public synchronized void update(long key, Record r) {
        long position = keyToPosition(key);
        heap.write(position,r.getData(),r.size());
        this.handler.updateRecord(r,positionToKey(position));
        heap.commitWrites();
    }

    @Override
    public synchronized void delete(long key) {
        if(key!=qtdOfRecords-1){
            byte[] buffer = new byte[sizeOfEachRecord];
            long position = keyToPosition(qtdOfRecords-1);
            heap.read(position,buffer,0,sizeOfEachRecord);
            position = keyToPosition(key);
            heap.write(position,buffer,0,sizeOfEachRecord);
        }
        qtdOfRecords -=1;
        heap.commitWrites();
    }

    @Override
    public synchronized void close() {
        this.flush();
        heap.getFileManager().close();
        heap = null;
    }

}
