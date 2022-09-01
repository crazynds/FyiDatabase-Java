package engine.virtualization.record.manager.storage.btree;

import engine.file.buffers.BlockBuffer;
import engine.file.streams.BlockStream;
import engine.virtualization.interfaces.BlockManager;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordInterface;

public class BTreeHandler {

    private BlockManager blockManager;
    protected BlockStream stream;
    protected RecordInfoExtractor ri;

    private int sizeOfPk;
    private int sizeOfEntry;

    public BTreeHandler(BlockStream stream,RecordInfoExtractor ri,BlockManager blockManager, int sizeOfPk, int sizeOfEntry){
        this.blockManager=blockManager;
        this.sizeOfEntry=sizeOfEntry;
        this.sizeOfPk=sizeOfPk;
        this.stream = stream;
        this.ri = ri;
    }


    public BlockManager getBlockManager() {
        return blockManager;
    }

    public int getSizeOfPk() {
        return sizeOfPk;
    }

    public int getSizeOfEntry() {
        return sizeOfEntry;
    }

    public BlockStream getStream() {
        return stream;
    }

    public RecordInfoExtractor getRi() {
        return ri;
    }
}
