package engine.virtualization.record.manager;

import engine.exceptions.NotFoundRowException;
import engine.file.FileManager;
import engine.file.streams.ReadByteStream;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.storage.FixedRecordStorage;
import engine.virtualization.record.manager.storage.OptimizedFixedRecordStorage;

import java.math.BigInteger;
import java.util.List;
import java.util.TreeMap;

public class FixedRecordManager extends RecordManager{


    private TreeMap<BigInteger,Long> recordMap= new TreeMap<>();
    private FixedRecordStorage recordStorage;

    private RecordInterface customInterface;


    private int sizeOfEachRecord;


    public FixedRecordManager(FileManager fm, RecordInterface ri, int sizeOfEachRecord) {
        super(fm, ri);
        customInterface = new AuxRecordInterface();
        recordStorage = new FixedRecordStorage(fm,customInterface,sizeOfEachRecord);
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
        recordMap.clear();
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
        if(recordMap.containsKey(pk)){
            Long pos = recordMap.get(pk);
            recordStorage.read(pos,buffer);
        }else{
            boolean b = recordStorage.search(pk,buffer);
            if(!b)
                throw new NotFoundRowException("FixedRecordManager->Read",pk);
        }
    }

    @Override
    public void write(Record r) {
        BigInteger pk = recordInterface.getPrimaryKey(r);

        if(recordMap.containsKey(pk)){
            /*
             * Nessa função vc tem que ter certeza de qual a posição real do item
             * porém garante um maior desempenho
             */
            Long pos = recordMap.get(pk);
            recordStorage.write(r,pos);
        }else{
            /*
             * Essa função já garante que o item não sera duplicado
             * porém tem um menor desempenho
             */
            recordStorage.writeNew(r);
        }
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

    private class AuxRecordInterface implements RecordInterface{

        private RecordInterface origin = getRecordInterface();

        @Override
        public BigInteger getPrimaryKey(Record r)  {
            return origin.getPrimaryKey(r);
        }

        @Override
        public BigInteger getPrimaryKey(ReadByteStream rbs) {
            return origin.getPrimaryKey(rbs);
        }

        @Override
        public boolean isActiveRecord(Record r) {
            return origin.isActiveRecord(r);
        }

        @Override
        public boolean isActiveRecord(ReadByteStream rbs) {
            return origin.isActiveRecord(rbs);
        }

        @Override
        public void updeteReference(BigInteger pk, long key) {
            recordMap.put(pk,key);
        }

        @Override
        public void setActiveRecord(Record r, boolean active) {
            origin.setActiveRecord(r,active);
        }

    }
}
