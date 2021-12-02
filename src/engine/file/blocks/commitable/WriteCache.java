package engine.file.blocks.commitable;

import engine.info.Parameters;

public class WriteCache{
	byte[] data;
	long pos;

	public WriteCache(long pos,byte[] data, boolean clone) {
		if(clone) {
			this.data = data.clone();
			Parameters.MEMORY_ALLOCATED_BY_COMMITTABLE_BLOCKS +=this.data.length;
		}else
			this.data=data;
		this.pos=pos;
	}
	
	public WriteCache(long pos,byte[] data,int offset,int len) {
		this.data=new byte[len];
		System.arraycopy(data, offset, this.data, 0, len);
		Parameters.MEMORY_ALLOCATED_BY_COMMITTABLE_BLOCKS +=this.data.length;
		this.pos=pos;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public long getPos() {
		return pos;
	}
	
	
}