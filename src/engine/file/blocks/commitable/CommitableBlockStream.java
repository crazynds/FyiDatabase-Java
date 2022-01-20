package engine.file.blocks.commitable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedList;

import engine.exceptions.DataBaseException;
import engine.file.streams.WriteByteStream;

public class CommitableBlockStream implements WriteByteStream {

	private final short VALUE_TEST = 999;
	private final short FINAL_VALUE = 1000;

	private WriteBack commit;

	private short[] buffer;

	private long position=0;
	private int blockSize;

	public CommitableBlockStream(WriteBack commit,int blockSize) {
		this.blockSize = blockSize;
		this.commit=commit;
		buffer = new short[blockSize+1];
		Arrays.fill(buffer,VALUE_TEST);
		buffer[blockSize] = FINAL_VALUE;
	}

	@Override
	public int write(long pos, byte[] data, int len) {
		return write(pos, data, 0, len);
	}
	@Override
	public int write(long pos, byte[] data, int offset, int len)  {
		if(pos>=blockSize)return 0;
		if(data.length+offset<len){
			throw new DataBaseException("Block->write","Array passado é menor que o solicitado para escrever");
		}
		//WriteCache bw = new WriteCache(pos, data,offset,len);

		for(int x=0;x<len && pos+x<blockSize;x++){
			buffer[(int)pos+x] = (short) data[offset+x];
		}
		//changes.addLast(bw);
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
		LinkedList<WriteCache> changes = new LinkedList<WriteCache>();

		boolean isSet = false;
		int start = 0, x = 0;

		do{
			buffer[blockSize] = FINAL_VALUE;
			while(buffer[x]==VALUE_TEST)x++;
			start = x;

			buffer[blockSize] = VALUE_TEST;
			while(buffer[x]!=VALUE_TEST)x++;

			if(start!=x) {
				byte[] data = new byte[x-start];
				for(int y=start;y<x;y++){
					data[y-start] = (byte)buffer[y];
				}
				changes.add(new WriteCache(start, data,false));
			}
		}while(x<blockSize);
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
