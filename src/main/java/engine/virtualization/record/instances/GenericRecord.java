package engine.virtualization.record.instances;

import engine.exceptions.DataBaseException;
import engine.info.Parameters;
import engine.virtualization.record.Record;

import java.nio.ByteBuffer;

public class GenericRecord extends Record {

	private byte[] data;
	private int size;

	public GenericRecord(byte[] data){
		this.data=data;
		size=-1;
	}

	public GenericRecord(byte[] data,int size){
		this.data=data;
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
	public ByteBuffer read(long pos, int len) {
		if(pos+len>size()){
			len -= (pos+len)-size();
		}
		ByteBuffer buff = ByteBuffer.allocate(len);
		buff.put(0,this.data,(int)pos,len);
		return buff;
	}

	@Override
	public ByteBuffer readSeq(int len) {
		return read(pointer,len);
	}

	@Override
	public int read(long pos, byte[] buffer, int offset, int len) {
		if(pos+len>size()){
			len -= (pos+len)-size();
		}
		System.arraycopy(this.data, (int) pos,buffer,offset,len);
		return len;
	}

	@Override
	public int readSeq(byte[] buffer, int offset,int len) {
		int readed = read(pointer,buffer,offset,len);
		pointer+=readed;
		return readed;
	}

	@Override
	public int read(long pos, ByteBuffer buffer, int offset, int len) {
		return 0;
	}

	@Override
	public int readSeq(ByteBuffer buffer, int offset, int len) {
		return 0;
	}
}
