package engine.file.streams;

public interface WriteByteStream extends PointerStream{

	public int write(long pos,byte[] data,int len);
	public int write(long pos,byte[] data,int offset, int len);
	
	//uses pointer stream
	public int writeSeq(byte[] data,int offset, int len);
	
	public void commitWrites() ;
	
}
