package engine.file.blocks;

public interface BlockFace {

	public int getBlockSize();	
	public default boolean compareBlockFaces(BlockFace bf){
		return this.getBlockSize()==bf.getBlockSize();
	};
	
}
