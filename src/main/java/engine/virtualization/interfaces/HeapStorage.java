package engine.virtualization.interfaces;

import java.io.IOException;
import java.nio.ByteBuffer;

import engine.file.FileManager;
import engine.file.blocks.ReadableBlock;
import engine.file.buffers.FIFOBlockBuffer;
import engine.file.streams.ByteStream;
import engine.file.streams.ReadByteStream;
import engine.file.streams.WriteByteStream;

public class HeapStorage implements ByteStream{
	
	private FileManager origin;
	
	private TemporaryBuffer temp;


	private WriteByteStream actualWriter;
	private int blockWriter;

	
	private long pointer;


	public HeapStorage(FileManager origin,int tempBufferSize) throws IOException {
		temp = new TemporaryBuffer(origin,tempBufferSize);
		pointer = 0;
		this.origin=origin;
		origin.getBuffer().hintBlock(0);
		actualWriter = null;
		blockWriter = -1;
	}
		
	public HeapStorage(FileManager origin) throws IOException {
		this(origin,16);
	}
	
	

	public static void main(String[] args) throws IOException {
		FileManager fm = new FileManager("W:/test.dat",new FIFOBlockBuffer(12));
		fm.clearFile();
		HeapStorage h = new HeapStorage(fm);
		
		byte[] arr = "ola mundo".getBytes();
		for(int x=0;x<10000000;x++) {
			h.write(x * arr.length, arr, arr.length);
		}
		h.commitWrites();

		byte[] buff = new byte[arr.length];
		arr = "123 45678".getBytes();
		for(int x=0;x<5;x++) {
			h.write(x * arr.length, arr, arr.length);
			h.read(x*arr.length,buff,0,9);
			String str = new String(buff);
			System.out.println(str);
		}
		h.commitWrites();

		for(int x=0;x<7;x++) {
			h.read(x*arr.length,buff,0,9);
			String str = new String(buff);
			System.out.println(str);
		}

		fm.close();
	}

	public void clearFile(){
		origin.getBuffer().flush();
		temp.clear();
		origin.clearFile();
	}

	@Override
	public void setPointer(long pos) {
		pointer = pos;
	}

	@Override
	public long getPointer() {
		return pointer;
	}

	@Override
	public int write(long pos, byte[] data, int len) {
		return write(pos, data, 0, len);
	}

	@Override
	public synchronized int write(long pos, byte[] data, int offset, int len) {
		int block = (int) (pos/origin.getBlockSize());
		int position = (int) (pos%origin.getBlockSize());
		
		
		int offset2=0;
		do {
			if(blockWriter!=block){
				closeWriter();
				actualWriter = temp.getBlockWriteByteStream(block);
				blockWriter = block;
			}
			offset2+=actualWriter.write(position, data,offset+offset2, len-offset2);

			block++;
			position=0;
		}while(offset2<len);
		return offset2;
	}

	@Override
	public int writeSeq(byte[] data, int offset, int len) {
		int val = write(pointer, data, offset,len);
		pointer+=val;
		return val;
	}

	@Override
	public synchronized void commitWrites() {
		closeWriter();
		temp.commit();
	}

	private void closeWriter(){
		if(actualWriter!=null) {
			actualWriter.commitWrites();
			actualWriter = null;
			blockWriter = -1;
		}
	}

	@Override
	public ByteBuffer read(long pos, int len) {
		ByteBuffer buffer = ByteBuffer.allocate(len);
		read(pos, buffer,0,len);
		return buffer;
	}

	@Override
	public ByteBuffer readSeq(int len) {
		ByteBuffer buffer = ByteBuffer.allocate(len);
		pointer+=read(pointer, buffer,0,len);
		return buffer;
	}

	@Override
	public int read(long pos, byte[] buffer, int offset, int len) {
		int block = (int) (pos/origin.getBlockSize());
		int position = (int) (pos%origin.getBlockSize());
		
		int offset2=0;
		int leitura;
		do {
			ReadableBlock rbs = temp.getBlockReadByteStream(block);
			leitura = len-offset2;
			if(leitura+position>rbs.getBlockSize())leitura = rbs.getBlockSize()-position;
			rbs.read(position, buffer, offset2+offset, leitura);

			offset2+=leitura;
			block++;
			position=0;
		}while(offset2<len);
		return offset2;
	}

	

	@Override
	public int readSeq(byte[] buffer, int offset,int len) {
		int increase =read(pointer, buffer,offset,len);
		pointer+=increase;
		return increase;
	}

	@Override
	public int read(long pos, ByteBuffer buffer, int offset, int len) {
		int block = (int) (pos/origin.getBlockSize());
		int position = (int) (pos%origin.getBlockSize());

		int offset2=0;
		int leitura;
		do {
			ReadableBlock rbs = temp.getBlockReadByteStream(block);
			leitura = len-offset2;
			if(leitura+position>rbs.getBlockSize())leitura = rbs.getBlockSize()-position;
			rbs.read(position, buffer, offset2+offset, leitura);

			offset2+=leitura;
			block++;
			position=0;
		}while(offset2<len);
		return offset2;
	}

	@Override
	public int readSeq(ByteBuffer buffer, int offset, int len) {
		int increase =read(pointer, buffer,offset,len);
		pointer+=increase;
		return increase;
	}

}
