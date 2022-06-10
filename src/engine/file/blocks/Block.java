package engine.file.blocks;

import engine.exceptions.DataBaseException;
import engine.file.streams.ByteStream;
import engine.info.Parameters;

import java.nio.ByteBuffer;

public class Block implements BlockFace,ByteStream{

	protected ByteBuffer buffer;
	protected int size;
	private long pointer=0;

	public Block(Block b,boolean clone) {
		if(clone) {
			this.buffer = ByteBuffer.allocateDirect(b.buffer.capacity());
			final ByteBuffer readOnly = b.buffer.asReadOnlyBuffer();
			readOnly.flip();
			this.buffer.put(readOnly);
			this.size=size;
			Parameters.MEMORY_ALLOCATED_BY_BLOCKS+=size;
		}else {
			this.buffer = b.buffer;
			this.size=buffer.capacity();
		}
	}
	public Block(ByteBuffer buffer) {
		this.buffer = buffer;
		this.size=buffer.capacity();
	}
	public Block(int size) {
		this.buffer = ByteBuffer.allocateDirect(size);
		this.size=size;
		Parameters.MEMORY_ALLOCATED_BY_BLOCKS+=size;
	}
	
	@Override
	public int getBlockSize() {
		return this.size;
	}
	
	public ByteBuffer getBuffer() {
		return this.buffer;
	}
	
	public void write(Block b){
		if(!this.compareBlockFaces(b))throw new DataBaseException("Block->write(block)","Tamanhos de blocos divergem ("+this.getBlockSize()+" != "+b.getBlockSize()+")");
		this.buffer.position(0);
		this.buffer.put(b.buffer.position(0).asReadOnlyBuffer().flip());
	}

	@Override
	public byte[] read(long pos, int len) throws  DataBaseException {
		if(pos>=this.size)throw new DataBaseException("GenericRecord->read","Posição inicial maior que o máximo");
		if(pos+len>this.size) {
			len =(int) (this.size -pos);
		}
		byte[] arr = new byte[len];
		this.buffer.get((int)pos,arr,0,len);
		Parameters.MEMORY_ALLOCATED_BY_BYTE_ARRAY+=arr.length;
		return arr;
	}

	@Override
	public int read(long pos, byte[] arr,int offset, int len)  {
		if(pos>=this.size)throw new DataBaseException("Block->read","Posição inicial maior que o máximo");
		if(arr.length-offset<len)throw new DataBaseException("Block->read", "Buffer passado é menor do que o que vai ser lido");
		if(pos+len>this.size) {
			len =(int) (this.size -pos);
		}
		this.buffer.get((int)pos,arr,offset,len);
		return len;
	}
	@Override
	public int readSeq(int len, byte[] buffer,int offset)  {
		int increase = read(pointer,buffer,offset,len);
		pointer+=increase;
		return increase;
	}

	@Override
	public byte[] readSeq(int len) throws  DataBaseException {
		byte[] arr = read(pointer, len);
		pointer+=arr.length;
		return arr;
	}
	
	@Override
	public int write(long pos, byte[] data, int offset, int len) throws  DataBaseException {
		if(pos>=this.size)throw new DataBaseException("Block->write","Posição inicial maior que o máximo");
		if(data.length<len)throw new DataBaseException("Block->write","Array passado é menor que o solicitado para escrever");
		if(pos+len>this.size) {
			len =(int) (this.size -pos);
		}
		this.buffer.put((int)pos,data,offset,len);
		return len;
	}

	@Override
	public int write(long pos, byte[] data, int len) {
		return this.write(pos, data,0, len);
	}

	@Override
	public int writeSeq(byte[] data, int offset, int len) throws  DataBaseException {
		int increase = this.write(pointer,data,offset,len);
		this.pointer+=increase;
		return increase;
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
	}
}
