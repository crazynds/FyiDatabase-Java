package engine.virtualization.record.manager;

import engine.file.FileManager;
import engine.file.streams.WriteByteStream;
import engine.virtualization.interfaces.BlockManager;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.storage.btree.BTreeStorage;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

public class FixedBTreeRecordManager extends RecordManager {

    private BTreeStorage btree;

    private int sizeOfPk,sizeOfEachRecord;

    public FixedBTreeRecordManager(FileManager fm, RecordInfoExtractor ri,int sizeOfPk, int sizeOfEachRecord) {
        super(fm, ri);

        this.sizeOfEachRecord = sizeOfEachRecord;
        this.sizeOfPk = sizeOfPk;

        btree = new BTreeStorage(fm,ri,new BlockManager(),sizeOfPk,sizeOfEachRecord);
        btree.load();
    }

    @Override
    public void restart() {
        btree = new BTreeStorage(fileManager,recordInterface,new BlockManager(),sizeOfPk,sizeOfEachRecord);
        fileManager.clearFile();
    }

    @Override
    public Record read(BigInteger pk) {
        ByteBuffer buff = btree.get(pk);

        if(buff==null)return null;

        byte[] arr = new byte[sizeOfEachRecord];
        buff.get(0,arr,0,buff.capacity());

        return new GenericRecord(arr);
    }

    @Override
    public void read(BigInteger pk, byte[] buffer) {
        ByteBuffer buff = btree.get(pk);
        if(buff==null)return;

        buff.get(0,buffer,0,buff.capacity());
    }

    @Override
    public void write(Record r) {
        BigInteger pk = recordInterface.getPrimaryKey(r);
        btree.insert(pk,ByteBuffer.wrap(r.getData()));
    }

    @Override
    public void write(List<Record> list) {
        for(Record r: list){
            write(r);
        }
    }

    @Override
    public boolean isOrdened() {
        return true;
    }

    @Override
    public RecordStream sequencialRead() {
        return null;
    }

}
