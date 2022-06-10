package engine.file.streams;

public interface ReadByteStream extends PointerStream {

	public byte[] read(long pos,int len) ;
	public byte[] readSeq(int len) ;
	

	public int read(long pos,byte[] buffer,int offset,int len) ;
	public int readSeq(int len,byte[] buffer,int offset) ;
	
}
