package engine.virtualization.record.manager;

import engine.exceptions.NotFoundRowException;
import engine.file.FileManager;
import engine.file.streams.ReadByteStream;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtraction;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.storage.FixedRecordStorage;
import engine.virtualization.record.manager.storage.OptimizedFixedRecordStorage;

import java.math.BigInteger;
import java.util.List;
import java.util.TreeMap;

public class FixedRecordManager extends RecordManager{


    private FixedRecordStorage recordStorage;

    private RecordInterface customInterface;


    private int sizeOfEachRecord;


    public FixedRecordManager(FileManager fm, RecordInfoExtraction ri, int sizeOfEachRecord) {
        super(fm, ri);
        recordStorage = new FixedRecordStorage(fm, new RecordInterface() {
            @Override
            public void updeteReference(BigInteger pk, long key) {
            }
            @Override
            public BigInteger getPrimaryKey(Record r) {
                return ri.getPrimaryKey(r);
            }

            @Override
            public BigInteger getPrimaryKey(ReadByteStream rbs) {
                return ri.getPrimaryKey(rbs);
            }

            @Override
            public boolean isActiveRecord(Record r) {
                return ri.isActiveRecord(r);
            }

            @Override
            public boolean isActiveRecord(ReadByteStream rbs) {
                return ri.isActiveRecord(rbs);
            }

            @Override
            public void setActiveRecord(Record r, boolean active) {
                ri.setActiveRecord(r,active);
            }
        }, sizeOfEachRecord);
        //recordStorage = new OptimizedFixedRecordStorage(fm,customInterface,sizeOfEachRecord);
        this.sizeOfEachRecord = sizeOfEachRecord;
    }

    @Override
    public void restart() {
        recordStorage.restartFileSet();
    }

    @Override
    public void flush() {
        recordStorage.flush();
        super.flush();
    }

    @Override
    public void close() {
        this.flush();
        super.close();
    }

    @Override
    public Record read(BigInteger pk) {
        Record r = new GenericRecord(new byte[sizeOfEachRecord]);
        read(pk,r.getData());
        return r;
    }

    @Override
    public void read(BigInteger pk, byte[] buffer) {
        boolean b = recordStorage.search(pk,buffer);
        if(!b)
            throw new NotFoundRowException("FixedRecordManager->Read",pk);
    }

    @Override
    public void write(Record r) {
        BigInteger pk = recordInterface.getPrimaryKey(r);
        recordStorage.writeNew(r);
    }

    @Override
    public void write(List<Record> list) {
        recordStorage.writeNew(list);
    }

    @Override
    public boolean isOrdened() {
        return true;
    }

    @Override
    public RecordStream sequencialRead() {
        return recordStorage.sequencialRead();
    }

}
