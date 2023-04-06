package engine.file.buffers;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import engine.exceptions.DataBaseException;
import engine.file.blocks.Block;
import engine.file.blocks.BlockID;
import engine.file.blocks.ReadableBlock;
import engine.file.blocks.commitable.CommitableBlockStream;
import engine.file.blocks.commitable.WriteBack;
import engine.file.blocks.commitable.WriteCache;
import engine.file.streams.WriteByteStream;

public class FIFOBlockBuffer extends BlockBuffer {
	protected LinkedList<EntryBlock> blocks;
	protected int maxSize;
	
	public FIFOBlockBuffer(int bufferSize) {
		maxSize = (bufferSize<=0)?1:bufferSize;
		blocks = new LinkedList<EntryBlock>();
	}

	@Override
	public int lastBlock() {
		if(stream==null)throw new DataBaseException("FIFOBlockBuffer->lastBlock","BlockStream não definido!");
		int last = stream.lastBlock();
		for (EntryBlock e:
			 blocks) {
			if(e.getBlock().getBlockId()>last){
				last = e.getBlock().getBlockId();
			}
		}
		return last;
	}

	@Override
	public synchronized void flush()  {
		if(stream==null)throw new DataBaseException("FIFOBlockBuffer->flush","BlockStream não definido!");
		TreeMap<Integer, EntryBlock> tree = new TreeMap<Integer, FIFOBlockBuffer.EntryBlock>();
		
		try {
			for(EntryBlock eb:blocks){
				if(eb.isSaved()==false) {
					eb.setSaved(true);
					tree.put(eb.getBlock().getBlockId(), eb);
				}
			}
		}catch(Exception e) {}
		
		Entry<Integer, EntryBlock> entry;
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
			buffered = loadBlock(pos);
		}
		return new Block(buffered.getBlock(),true);
	}

	@Override
	public void readBlock(int pos, ByteBuffer buffer)  {
		EntryBlock buffered = getBlockInBuffer(pos);
		if(buffered==null) {
			buffered = loadBlock(pos);
		}
		buffer.put(0,buffered.getBlock().getBuffer(),0,
				(buffer.capacity()>getBlockSize())?getBlockSize(): buffer.capacity());
	}
	
	@Override
	public synchronized void writeBlock(int pos, Block b)  {
		EntryBlock buffered = getBlockInBuffer(pos);
		if(buffered==null) {
			if(blocks.size()>=maxSize) {
				buffered = removeItem();
				buffered.getBlock().changeBlockID(b.getBuffer(), pos);
				buffered.setSaved(false);
			}else {
				buffered=new EntryBlock(new BlockID(b, pos),false);
			}
			addItem(buffered);
			return;
		}
		buffered.setSaved(false);
		if(!buffered.getBlock().equals(b))
			buffered.getBlock().write(b);
	}

	@Override
	public WriteByteStream getBlockWriteByteStream(int block)  {
		EntryBlock buffered = getBlockInBuffer(block);
		if(buffered==null) {
			buffered = loadBlock(block);
		}
		CommitableBlockStream bc = new CommitableBlockStream(new WriteBack() {
			int blockToSave = block;
			@Override
			public void commitWrites(LinkedList<WriteCache> list) {
				EntryBlock buffered = getBlockInBuffer(blockToSave);
				if(buffered==null) {
					buffered = loadBlock(blockToSave);
				}
				while(list.size()>0) {
					WriteCache wc = list.removeFirst();
					buffered.getBlock().write(wc.getPos(), wc.getData(), wc.getData().length);
					buffered.setSaved(false);
				}
			}
		},buffered.getBlock().getBlockSize());
		return bc;
	}

	@Override
	public ReadableBlock getBlockReadByteStream(int block)  {
		EntryBlock buffered = getBlockInBuffer(block);
		if(buffered==null) {
			buffered = loadBlock(block);
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
			loadBlock(num);
		}catch(Exception e) {
		}
	}

	@Override
	public synchronized void clearBuffer() {
		for(EntryBlock e:blocks){
			e.setSaved(true);
			e.getBlock().changeBlockID(-1);
		}
	}

	protected synchronized EntryBlock loadBlock(int block)  {
		if(stream==null)throw new DataBaseException("FIFOBlockBuffer->loadBlock","BlockStream não definido!");
		EntryBlock usable = null;
		if(blocks.size()>=maxSize) {
			usable = removeItem();
			if(stream.lastBlock()>=block)
				stream.readBlock(block,usable.getBlock().getBuffer());
			usable.getBlock().changeBlockID(block);
			usable.setSaved(true);
		}else {
			BlockID b = new BlockID(ByteBuffer.allocateDirect(getBlockSize()),block);
			if(stream.lastBlock()>=block)
				stream.readBlock(block, b.getBuffer());
			usable = new EntryBlock(b, true);
		}
		blocks.addLast(usable);
		return usable;
	}
	
	protected synchronized void addItem(EntryBlock e) {
		blocks.addLast(e);
	}
	
	protected synchronized EntryBlock removeItem()  {
		if(stream==null)throw new DataBaseException("FIFOBlockBuffer->removeItem","BlockStream não definido!");
		EntryBlock e = blocks.removeFirst();
		if(e.isSaved()==false)
			stream.writeBlock(e.getBlock().getBlockId(), e.getBlock());
		e.setSaved(true);
		return e;
	}
	
	protected synchronized EntryBlock getBlockInBuffer(int block) {
		EntryBlock e=null;
		//ListIterator<EntryBlock> itr = blocks.listIterator();
		Iterator<EntryBlock> itr = blocks.descendingIterator();
		while(itr.hasNext()) {
			e=itr.next();
			if(e.getBlock().getBlockId()==block) {
				itr.remove();
				break;
			}else e=null;
		}
		if(e!=null)
			blocks.addLast(e);
		return e;
	}

	protected class EntryBlock implements Callable<Void>{
		private BlockID b;
		private Boolean saved;
		
		public EntryBlock(BlockID b, Boolean isSaved){
			this.b=b;
			this.saved=isSaved;
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

		@Override
		public Void call() throws Exception {
			setSaved(false);
			return null;
		}
	}
}
