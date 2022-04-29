package engine.file.streams;

import engine.file.Block;
import engine.file.FileConector;
import engine.file.blocks.BlockFace;

public interface ReadBlockStream extends FileConector,BlockFace {
	public Block readBlock(int pos);
	public void readBlock(int pos,byte[] buffer);//Retorna o valor de um bloco em um byte array
	
	public ReadByteStream getBlockReadByteStream(int block) ;

}
