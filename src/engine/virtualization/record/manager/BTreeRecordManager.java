package engine.virtualization.record.manager;

import engine.file.FileManager;
import engine.file.streams.WriteByteStream;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordStream;

import java.math.BigInteger;
import java.util.List;

public class BTreeRecordManager extends RecordManager {

    public BTreeRecordManager(FileManager fm, RecordInfoExtractor ri) {
        super(fm, ri);

        //Get data from header

        if(fm.lastBlock()<=1){
            this.start();
        }
    }

    private void start(){
        // prepare to write the header file
        WriteByteStream wbs = fileManager.getBlockWriteByteStream(0);

        //start block node in block 1

    }

    @Override
    public void restart() {

    }

    @Override
    public Record read(BigInteger pk) {
        return null;
    }

    @Override
    public void read(BigInteger pk, byte[] buffer) {

    }

    @Override
    public void write(Record r) {

    }

    @Override
    public void write(List<Record> list) {

    }

    @Override
    public boolean isOrdened() {
        return false;
    }

    @Override
    public RecordStream sequencialRead() {
        return null;
    }

}
