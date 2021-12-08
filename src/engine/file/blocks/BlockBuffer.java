package engine.file.blocks;

import engine.file.Block;
import engine.file.streams.BlockStream;

public abstract class BlockBuffer implements BlockStream{

	public abstract void startBuffering(BlockStream stream);
	
	public abstract Block getBlockIfExistInBuffer(int num);//Retorna um bloco
	public abstract void hintBlock(int num);//Avisa que determinado bloco será necessário para manter em cache
	public abstract void forceBlock(int num);//Força um bloco em cache

	public abstract void clearBuffer(); // Limpa o buffer independente se é necessário salvar ou não

}
