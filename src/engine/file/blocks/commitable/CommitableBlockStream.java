package engine.file.blocks.commitable;

import java.util.LinkedList;

import engine.exceptions.DataBaseException;
import engine.file.streams.WriteByteStream;

public class CommitableBlockStream implements WriteByteStream {

	private WriteBack commit;
	private LinkedList<WriteCache> changes = new LinkedList<WriteCache>();

	private long position=0;
	private int blockSize;

	public CommitableBlockStream(WriteBack commit,int blockSize) {
		this.blockSize = blockSize;
		this.commit=commit;
	}

	@Override
	public int write(long pos, byte[] data, int len) {
		return write(pos, data, 0, len);
	}
	@Override
	public int write(long pos, byte[] data, int offset, int len)  {
		if(pos>=blockSize)return 0;
		if(data.length+offset<len)throw new DataBaseException("Block->write","Array passado é menor que o solicitado para escrever");
		WriteCache bw = new WriteCache(pos, data,offset,len);
		changes.addLast(bw);
		if(pos+len>blockSize)return (int) (blockSize-pos);
		else return len;
	}
	
	@Override
	public int writeSeq(byte[] data, int offset, int len)  {
		long pos = getPointer();
		int val = write(pos,data,offset,len);
		setPointer(pos+val);
		return val;
	}
	
	
	@Override
	public void commitWrites()  {
		commit.commitWrites(changes);
	}


	@Override
	public void setPointer(long pos) {
		this.position=pos;
	}

	@Override
	public long getPointer() {
		return position;
	}
	

}
