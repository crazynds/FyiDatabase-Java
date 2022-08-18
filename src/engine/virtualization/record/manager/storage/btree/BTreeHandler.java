package engine.virtualization.record.manager.storage.btree;

import engine.virtualization.interfaces.BlockManager;

public class BTreeHandler {

    private BlockManager blockManager;

    private int sizeOfPk;
    private int sizeOfEntry;

    public BTreeHandler(BlockManager blockManager,int sizeOfPk, int sizeOfEntry){
        this.blockManager=blockManager;
        this.sizeOfEntry=sizeOfEntry;
        this.sizeOfPk=sizeOfPk;
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
}
