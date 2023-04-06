package engine.file.buffers;

import engine.file.blocks.Block;
import engine.file.streams.BlockStream;

public abstract class BlockBuffer implements BlockStream{

	protected BlockStream stream;

	public BlockStream getBlockStream(){
		return stream;
	}
	public void startBuffering(BlockStream stream){
		this.stream=stream;
	}
	
	public abstract Block getBlockIfExistInBuffer(int num);//Retorna o bloco se o mesmo estiver em algum buffer
	public abstract void hintBlock(int num);//Da a dica a classe que determinado bloco pode ser util no futuro e que deverá ser carregado
	public abstract void forceBlock(int num);//Força um bloco em cache

	public abstract void clearBuffer(); // Limpa o buffer independente se é necessário salvar ou não
}
