package engine.virtualization.record.manager.storage.btree;

import engine.exceptions.DataBaseException;
import engine.file.blocks.ReadableBlock;
import engine.file.buffers.BlockBuffer;
import engine.file.streams.BlockStream;
import engine.file.streams.ReferenceReadByteStream;
import engine.file.streams.WriteByteStream;
import engine.util.Util;
import engine.virtualization.interfaces.BlockManager;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordInterface;
import lib.btree.BPlusTreeInsertionException;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public class BTreeStorage implements Iterable<Map.Entry<BigInteger,ByteBuffer>>{

    private Node rootNode;
    private Leaf leafNode;

    protected BTreeHandler handler;

    public BTreeStorage(BlockStream stream, RecordInfoExtractor ri, BlockManager blockManager, int sizeOfPk, int sizeOfEntry){
        this.handler = new BTreeHandler(stream,ri,blockManager,sizeOfPk,sizeOfEntry);
        this.handler.getBlockManager().setNode(0);
        this.leafNode = new Leaf(this.handler,blockManager.allocNew());
        rootNode = leafNode;
    }

    public void load(){
        ReadableBlock readable = handler.getStream().getBlockReadByteStream(0);

        readable.setPointer(0);

        byte ident = Util.convertByteBufferToNumber(readable.readSeq(1)).byteValue();
        if(ident!=-1)return;

        int blockRoot = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
        int blockLeaf = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
        int maxBlock = handler.getStream().lastBlock();

        if(blockRoot <=0 || blockLeaf <= 0 || blockRoot >maxBlock || blockLeaf>maxBlock)return;
        try {
            if (blockLeaf == blockRoot) {
                leafNode = (Leaf) this.handler.loadNode(blockLeaf);
                rootNode = leafNode;
            } else {
                rootNode = this.handler.loadNode(blockRoot);
                leafNode = (Leaf) this.handler.loadNode(blockLeaf);
            }
        }catch (DataBaseException dbe){
            this.leafNode = new Leaf(this.handler,this.handler.getBlockManager().allocNew());
            rootNode = leafNode;
        }
    }

    public void save(){
        WriteByteStream wbs = handler.getStream().getBlockWriteByteStream(0);
        wbs.setPointer(0);
        wbs.writeSeq(new byte[]{-1},0,1);
        wbs.writeSeq(Util.convertLongToByteArray(rootNode.block,4),0,4);
        wbs.writeSeq(Util.convertLongToByteArray(leafNode.block,4),0,4);

        rootNode.save();

        wbs.commitWrites();
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



    public void print(){
        rootNode.print(0);
    }

    public ByteBuffer get(BigInteger t){
        return rootNode.get(t);
    }
    public ByteBuffer remove(BigInteger t){
        return rootNode.remove(t);
    }

    @Override
    public Iterator<Map.Entry<BigInteger, ByteBuffer>> iterator() {
        return this.iterator(leafNode.min());
    }

    public Iterator<Map.Entry<BigInteger, ByteBuffer>> iterator(BigInteger pk) {
        return new Iterator<Map.Entry<BigInteger, ByteBuffer>>() {

            Leaf currentLeaf = rootNode.leafFrom(pk);
            Iterator<Map.Entry<BigInteger, ByteBuffer>> it = null;

            boolean updateLeaf(){
                while((it==null || !it.hasNext()) && currentLeaf!=null){
                    it = currentLeaf.iterator(pk);
                    currentLeaf = currentLeaf.getNextLeaf();
                    System.out.print("|");
                }
                if(it!=null && it.hasNext())return true;
                return false;
            }
            @Override
            public boolean hasNext() {
                if(!updateLeaf())return false;
                return true;
            }

            @Override
            public Map.Entry<BigInteger, ByteBuffer> next() {
                if(!updateLeaf())return null;
                return it.next();
            }
        };
    }
}
