package engine.file.buffers;

import engine.exceptions.DataBaseException;
import engine.file.blocks.Block;
import engine.file.blocks.BlockID;
import engine.file.blocks.ReadableBlock;
import engine.file.blocks.commitable.CommitableBlockStream;
import engine.file.blocks.commitable.WriteBack;
import engine.file.blocks.commitable.WriteCache;
import engine.file.streams.BlockStream;
import engine.file.streams.WriteByteStream;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class OptimizedFIFOBlockBuffer extends BlockBuffer {
    protected EntryBlock[] entries;
    protected TreeMap<Integer,Integer> blockMaping;
    protected TreeMap<Long,Integer> timeMaping;

    protected int maxSize;

    public OptimizedFIFOBlockBuffer(int bufferSize) {
        maxSize = (bufferSize<=0)?1:bufferSize;

        entries = new EntryBlock[bufferSize];
        blockMaping = new TreeMap<>();
        blockMaping.put(-1,-1);
        timeMaping = new TreeMap<>();
    }

    @Override
    public void startBuffering(BlockStream stream) {
        super.startBuffering(stream);
        for(int x=0;x<entries.length;x++){
            entries[x] = new EntryBlock(new BlockID(new Block(stream.getBlockSize()),-1),true);
            timeMaping.put(entries[x].getTime()+x,x);
        }
    }

    @Override
    public int lastBlock()  {
        if(stream==null)throw new DataBaseException("FIFOBlockBuffer->lastBlock","BlockStream não definido!");
        int last = stream.lastBlock();
        int last2 = blockMaping.lastKey();
        return (last>last2)?last:last2;
    }

    @Override
    public synchronized void flush() {
        if(stream==null)throw new DataBaseException("FIFOBlockBuffer->flush","BlockStream não definido!");
        TreeMap<Integer, EntryBlock> tree = new TreeMap<Integer, EntryBlock>();

        for (Map.Entry<Integer,Integer> x:
                blockMaping.entrySet()) {
            if(x.getKey()>=0 && !entries[x.getValue()].isSaved()) {
                entries[x.getValue()].setSaved(true);
                tree.put(x.getKey(), entries[x.getValue()]);
            }
        }

        Map.Entry<Integer, EntryBlock> entry;
        while((entry=tree.pollFirstEntry())!=null) {
            stream.writeBlock(entry.getKey(), entry.getValue().getBlock());
        }
        stream.flush();
    }


    @Override
    public void close() {
        try {
            this.flush();
        } catch (DataBaseException e) {
        }
    }


    @Override
    public int getBlockSize() {
        if(stream==null)return 0;
        return stream.getBlockSize();
    }


    @Override
    public Block readBlock(int pos)  {
        EntryBlock buffered = getBlockInBuffer(pos);
        if(buffered==null) {
            buffered = loadBlock(pos,true);
        }
        return new Block(buffered.getBlock(),true);
    }

    @Override
    public void readBlock(int pos, ByteBuffer buffer)  {
        EntryBlock buffered = getBlockInBuffer(pos);
        if(buffered==null)
            buffered = loadBlock(pos,true);
        buffer.put(0,buffered.getBlock().getBuffer(),0,
                (buffer.capacity()>getBlockSize())?getBlockSize(): buffer.capacity());
    }

    @Override
    public synchronized void writeBlock(int pos, Block b)  {
        EntryBlock buffered = getBlockInBuffer(pos);
        if(buffered==null)
            buffered = loadBlock(pos,false);
        buffered.setSaved(false);
        if(!buffered.getBlock().equals(b))
            buffered.getBlock().write(b);
    }

    @Override
    public WriteByteStream getBlockWriteByteStream(int block)  {
        CommitableBlockStream bc = new CommitableBlockStream(new WriteBack() {
            int blockToSave = block;
            @Override
            public void commitWrites(LinkedList<WriteCache> list) {
                writableCommit(blockToSave,list);
            }
        },stream.getBlockSize());
        return bc;
    }
    private synchronized void writableCommit(int blockToSave,LinkedList<WriteCache> list){
        EntryBlock buffered = getBlockInBuffer(blockToSave);
        if(buffered==null)
            buffered = loadBlock(blockToSave,true);
        if(list.size()>0)
            buffered.setSaved(false);
        WriteCache wc;
        BlockID block = buffered.getBlock();
        while(list.size()>0) {
            wc = list.removeFirst();
            block.write(wc.getPos(), wc.getData(), wc.getData().length);
        }
    }


    @Override
    public ReadableBlock getBlockReadByteStream(int block)  {
        EntryBlock buffered = getBlockInBuffer(block);
        if(buffered==null) {
            buffered = loadBlock(block,true);
        }
        return buffered.getBlock();
    }




    @Override
    public Block getBlockIfExistInBuffer(int num) {
        EntryBlock eb = getBlockInBuffer(num);
        if(eb==null)return null;
        else return eb.getBlock();
    }


    @Override
    public void hintBlock(int num) {
        forceBlock(num);
    }


    @Override
    public void forceBlock(int num) {
        if(getBlockInBuffer(num)!=null)return;
        try {
            loadBlock(num,true);
        }catch(Exception e) {
        }
    }

    @Override
    public synchronized void clearBuffer() {
        flush();
        for(EntryBlock e:entries){
            e.setSaved(true);
            if(e.getBlock().getBlockId()>=0)
                blockMaping.remove(e.getBlock().getBlockId());
            e.getBlock().changeBlockID(-1);
        }
    }


    protected synchronized EntryBlock loadBlock(int block,boolean read)  {
        if(stream==null)throw new DataBaseException("FIFOBlockBuffer->loadBlock","BlockStream não definido!");
        Map.Entry<Long,Integer> entry = timeMaping.pollFirstEntry();
        EntryBlock usable = entries[entry.getValue()];
        if(usable.getBlock().getBlockId()>=0){
            blockMaping.remove(usable.getBlock().getBlockId());
            if(usable.isSaved()==false){
                stream.writeBlock(usable.getBlock().getBlockId(), usable.getBlock());
            }
        }
        if(read && stream.lastBlock()>=block) {
            stream.readBlock(block, usable.getBlock().getBuffer());
        }
        usable.getBlock().changeBlockID(block);
        usable.setTime(System.nanoTime());
        usable.setSaved(true);
        timeMaping.put(usable.getTime(),entry.getValue());
        blockMaping.put(usable.getBlock().getBlockId(),entry.getValue());
        return usable;
    }

    protected synchronized EntryBlock getBlockInBuffer(int block) {
        Integer i = blockMaping.get(block);
        if(i==null)return null;
        return entries[i];
    }


    protected class EntryBlock implements Callable<Void> {
        private BlockID b;
        private Boolean saved;
        private long time;

        public EntryBlock(BlockID b, Boolean isSaved){
            this.b=b;
            this.saved=isSaved;
            time = System.nanoTime();
        }

        public BlockID getBlock() {
            return b;
        }
        public Boolean isSaved() {
            return saved;
        }
        public void setSaved(Boolean saved) {
            this.saved=saved;
        }
        public long getTime(){
            return time;
        }
        public void setTime(long time){
            this.time=time;
        }

        @Override
        public Void call() throws Exception {
            setSaved(false);
            return null;
        }
    }

}
