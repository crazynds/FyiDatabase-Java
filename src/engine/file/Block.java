package engine.file;

import engine.exceptions.DataBaseException;
import engine.file.blocks.BlockFace;
import engine.file.streams.ByteStream;
import engine.info.Parameters;

public class Block implements BlockFace,ByteStream{
	
	protected byte[] data;	
	private long pointer=0;

	public Block(Block b,boolean clone) {
		if(clone) {
			this.data =b.data.clone();
			Parameters.MEMORY_ALLOCATED_BY_BLOCKS+=data.length;
		}else this.data=b.data;
	}
	public Block(byte[] data) {
		this.data =data;
		Parameters.MEMORY_ALLOCATED_BY_BLOCKS+=data.length;
	}
	public Block(int size) {
		this.data =new byte[size];
		Parameters.MEMORY_ALLOCATED_BY_BLOCKS+=data.length;
	}
	
	@Override
	public int getBlockSize() {
		return data.length;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public void write(Block b){
		if(!this.compareBlockFaces(b))throw new DataBaseException("Block->write(block)","Tamanhos de blocos divergem ("+this.getBlockSize()+" != "+b.getBlockSize()+")");
		System.arraycopy(b.getData(), 0, this.data, 0, b.getBlockSize());
	}

	@Override
	public byte[] read(long pos, int len) throws  DataBaseException {
		if(pos>=data.length)throw new DataBaseException("GenericRecord->read","Posição inicial maior que o máximo");
		byte[] arr;
		if(pos+len>data.length) {
			len =(int) (data.length -pos);
			arr = new byte[len];
			read(pos,len,arr,0);
		}else {
			arr = new byte[len];
			read(pos,len,arr,0);
		}
		Parameters.MEMORY_ALLOCATED_BY_BYTE_ARRAY+=arr.length;
		return arr;
	}

	@Override
	public int read(long pos, int len, byte[] arr,int offset)  {
		if(pos>=data.length)throw new DataBaseException("Block->read","Posição inicial maior que o máximo");
		if(arr.length-offset<len)throw new DataBaseException("Block->read", "Buffer passado é menor do que o que vai ser lido");
		if(pos+len>data.length) {
			len =(int) (data.length -pos);
		}
		System.arraycopy(data,(int) pos, arr, offset, len);
		return len;
	}
	@Override
	public int readSeq(int len, byte[] buffer,int offset)  {
		int increase = read(pointer, len,buffer,offset);
		pointer+=increase;
		return increase;
	}
	
	@Override
	public int write(long pos, byte[] data, int offset, int len) throws  DataBaseException {
		if(pos>=this.data.length)throw new DataBaseException("Block->write","Posição inicial maior que o máximo");
		if(data.length<len)throw new DataBaseException("Block->write","Array passado é menor que o solicitado para escrever");
		if(pos+len>this.data.length) {
			System.arraycopy(data, offset, this.data, (int)pos, (int) (this.data.length-pos));
			return (int) (this.data.length-pos);
		}else {
			System.arraycopy(data, offset, this.data, (int)pos, len);
			return len;
		}
	}

	@Override
	public int write(long pos, byte[] data, int len) {
		return this.write(pos, data,0, len);
	}
	

	@Override
	public byte[] readSeq(int len) throws  DataBaseException {
		byte[] arr = read(pointer, len);
		pointer+=arr.length;
		return arr;
	}
	@Override
	public int writeSeq(byte[] data, int offset, int len) throws  DataBaseException {
		int pos = (int) pointer;
		if(pos>=this.data.length)throw new DataBaseException("Block->write","Posição inicial maior que o máximo");
		if(data.length<len)throw new DataBaseException("Block->write","Array passado é menor que o solicitado para escrever");
		if(pos+len>this.data.length) {
			System.arraycopy(data, offset, this.data, pos, this.data.length-pos);
			pointer+=this.data.length-pos;
			return this.data.length-pos;
		}else {
			System.arraycopy(data, offset, this.data, pos, len);
			pointer+=len;
			return len;
		}
	}
	
	@Override
	public void setPointer(long pos) {
		this.pointer=pos;
	}
	@Override
	public long getPointer() {
		return this.pointer;
	}
	
	@Override
	public void commitWrites()  {
		//All writes is already commited's, no necessary code
	}

}
