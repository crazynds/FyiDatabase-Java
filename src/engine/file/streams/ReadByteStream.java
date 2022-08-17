package engine.file.streams;

import java.nio.ByteBuffer;

public interface ReadByteStream extends PointerStream {

	public ByteBuffer read(long pos,int len);
	public ByteBuffer readSeq(int len);


	public int read(long pos,byte[] buffer,int offset,int len);
	public int readSeq(byte[] buffer,int offset,int len);
	public int read(long pos, ByteBuffer buffer, int offset, int len);
	public int readSeq(ByteBuffer buffer,int offset,int len);
	
}
