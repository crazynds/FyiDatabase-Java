package engine.file.streams;


public interface BlockStream extends WriteBlockStream,ReadBlockStream{
	
	public int lastBlock() ;
}
