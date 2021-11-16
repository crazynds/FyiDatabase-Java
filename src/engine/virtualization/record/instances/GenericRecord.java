package engine.virtualization.record.instances;

import engine.exceptions.DataBaseException;
import engine.info.Parameters;
import engine.virtualization.record.Record;

public class GenericRecord extends Record {

	private byte[] data;
	private int size;

	public GenericRecord(byte[] data){
		this.data=data;
		Parameters.MEMORY_ALLOCATED_BY_RECORDS+=data.length;
		size=-1;
	}

	public GenericRecord(byte[] data,int size){
		this.data=data;
		Parameters.MEMORY_ALLOCATED_BY_RECORDS+=data.length;
		this.size= size;
	}

	@Override
	public byte[] getData(){
		return data;
	}

	@Override
	public int size() {
		if(size<0)return data.length;
		return size;
	}


	@Override
	public byte pos(int x) {
		return data[x];
	}
	@Override
	public void set(int x,byte b) {
		data[x]=b;
	}

	@Override
	public byte[] read(long pos, int len) {
		return new byte[0];
	}

	@Override
	public byte[] readSeq(int len) {
		return new byte[0];
	}

	@Override
	public int read(long pos, int len, byte[] buffer, int offset) {
		return 0;
	}

	@Override
	public int readSeq(int len, byte[] buffer, int offset) {
		return 0;
	}
}
