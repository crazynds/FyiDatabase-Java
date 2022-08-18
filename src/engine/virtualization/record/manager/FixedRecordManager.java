package engine.virtualization.record.manager;

import engine.exceptions.NotFoundRowException;
import engine.file.FileManager;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.storage.FixedRecordStorage;
import engine.virtualization.record.manager.storage.OptimizedFixedRecordStorage;

import java.math.BigInteger;
import java.util.List;

public class FixedRecordManager extends RecordManager{


    private FixedRecordStorage recordStorage;
    private int sizeOfEachRecord;


    public FixedRecordManager(FileManager fm, RecordInterface ri, int sizeOfEachRecord) {
        super(fm, ri.getExtractor());
        recordStorage = new OptimizedFixedRecordStorage(fm,ri, sizeOfEachRecord);
        this.sizeOfEachRecord = sizeOfEachRecord;
    }

    public FixedRecordManager(FileManager fm, RecordInfoExtractor ri, int sizeOfEachRecord) {
        super(fm, ri);
        recordStorage = new OptimizedFixedRecordStorage(fm,new RecordInterface(ri), sizeOfEachRecord);
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
