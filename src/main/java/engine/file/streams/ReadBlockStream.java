package engine.file.streams;

import engine.file.blocks.Block;
import engine.file.FileConector;
import engine.file.blocks.BlockFace;
import engine.file.blocks.ReadableBlock;

import java.nio.ByteBuffer;

public interface ReadBlockStream extends FileConector,BlockFace {
	public Block readBlock(int pos);
	public void readBlock(int pos, ByteBuffer buffer);//Retorna o valor de um bloco em um byte array
	
	public ReadableBlock getBlockReadByteStream(int block);

}
