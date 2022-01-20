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

	public void setData(byte[] data){
		this.data = data;
		this.size = data.length;
	}

	@Override
	public byte[] getData(){
		return data;
	}

	@Override
	public int size() {
		if (size < 0) return data.length;
		return size;
	}

	private long pointer=0;

	@Override
	public byte[] read(long pos, int len) {
		if(pos+len>size()){
			len -= (pos+len)-size();
		}
		byte[] buff = new byte[len];
		System.arraycopy(this.data, (int) pos,buff,0,len);
		return buff;
	}

	@Override
	public byte[] readSeq(int len) {
		return read(pointer,len);
	}

	@Override
	public int read(long pos, int len, byte[] buffer, int offset) {
		if(pos+len>size()){
			len -= (pos+len)-size();
		}
		System.arraycopy(this.data, (int) pos,buffer,offset,len);
		return len;
	}

	@Override
	public int readSeq(int len, byte[] buffer, int offset) {
		int readed = read(pointer,len,buffer,offset);
		pointer+=readed;
		return readed;
	}
}
