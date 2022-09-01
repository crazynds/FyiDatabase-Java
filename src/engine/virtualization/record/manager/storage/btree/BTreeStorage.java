package engine.virtualization.record.manager.storage.btree;

import engine.file.buffers.BlockBuffer;
import engine.file.streams.BlockStream;
import engine.virtualization.interfaces.BlockManager;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordInterface;
import lib.btree.BPlusTreeInsertionException;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class BTreeStorage {

    private Node rootNode;

    protected BTreeHandler handler;

    public BTreeStorage(BlockStream stream, RecordInfoExtractor ri, BlockManager blockManager, int sizeOfPk, int sizeOfEntry){
        this.handler = new BTreeHandler(stream,ri,blockManager,sizeOfPk,sizeOfEntry);
    }


    public void insert(BigInteger pk, ByteBuffer buff){
        try {
            rootNode.insert(pk, buff);
        }catch (BPlusTreeInsertionException e){
            Node left = rootNode;
            Node right = rootNode.half();
            Page page = new Page(handler, handler.getBlockManager().allocNew(),left);

            page.insertNode(right);

            rootNode = page;
            this.insert(pk,buff);
        }
    }

    public void load(){

    }

    public void save(){
        rootNode.save();
    }

    public void print(){
        rootNode.print(0);
    }

    public ByteBuffer get(BigInteger t){
        return rootNode.get(t);
    }
    public ByteBuffer remove(BigInteger t){
        return rootNode.remove(t);
    }
}
