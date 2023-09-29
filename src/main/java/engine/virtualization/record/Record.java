package engine.virtualization.record;

import engine.file.streams.ReadByteStream;
import lib.BigKey;


public abstract class Record implements ReadByteStream{

	private long pointer = 0;
	
	public Record() {
	}
	
	public abstract int size();

	public abstract byte[] getData();

	public abstract BigKey primaryKey();
	
	@Override
	public void setPointer(long pos) {
		this.pointer=pos;
	}
	
	@Override
	public long getPointer() {
		return this.pointer;
	}
	
}
