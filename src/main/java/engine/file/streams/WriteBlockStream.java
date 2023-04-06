package engine.file.streams;

import engine.file.blocks.Block;
import engine.file.FileConector;
import engine.file.blocks.BlockFace;

public interface WriteBlockStream extends FileConector,BlockFace {
	public void writeBlock(int pos,Block b) ;

	public WriteByteStream getBlockWriteByteStream(int block) ;
}
