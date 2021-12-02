package engine.virtualization.record.instances;

import engine.virtualization.record.Record;

public class PointerArrayRecord extends Record {
	private byte[] arr;
	private int offset,size;
	
	public PointerArrayRecord(byte []arr,int offset,int size) {
		this.arr=arr;
		this.offset=offset;
		this.size=size;
		if(arr.length<offset+size)throw new ArrayIndexOutOfBoundsException();
	}
/*
	@Override
	public byte pos(int x) {
		return arr[offset+x];
	}

	@Override
	public void set(int x, byte b) {
		arr[offset+x]=b;
	}
 */

	public byte[] getData(){
		byte[] buff = new byte[size];

		for(int x=0;x<size;x++){
			buff[x]=arr[offset+x];
		}

		return buff;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public byte[] read(long pos, int len)  {
		return null;
	}

	@Override
	public byte[] readSeq(int len)  {
		return null;
	}

	@Override
	public int read(long pos, int len, byte[] buffer, int offset)  {
		return 0;
	}

	@Override
	public int readSeq(int len, byte[] buffer, int offset) {
		return 0;
	}

	@Override
	public void setPointer(long pos) {
		
	}

	@Override
	public long getPointer() {
		return 0;
	}

}
