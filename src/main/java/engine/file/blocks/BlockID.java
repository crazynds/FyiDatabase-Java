package engine.file.blocks;

import engine.exceptions.DataBaseException;

import java.nio.ByteBuffer;

public class BlockID extends Block {

	private int blockId;

	public BlockID(ByteBuffer buffer,int id) {
		super(buffer);
		this.blockId=id;
	}

	public BlockID(Block b,int id) {
		super(b.getBlockSize(),true);
		write(b);
		this.blockId=id;
	}
	
	
	public int getBlockId() {
		return blockId;
	}

	public void changeBlockID(ByteBuffer data, int id) {
		if(getBlockSize()!=data.capacity())throw new DataBaseException("ChangeBlockID","Dados enviados possuem tamanho diferentes do necessário para reutilizar o bloco.");
		write(0, data.array(), 0, data.capacity());
		this.blockId=id;
	}
	public void changeBlockID(Block b,int id) {
		if(!this.compareBlockFaces(b))throw new DataBaseException("ChangeBlockID","Dados enviados possuem tamanho diferentes do necessário para reutilizar o bloco.");
		this.buffer=b.getBuffer();
		this.blockId=id;
	}
	public void changeBlockID(int id){
		this.blockId=id;
	}
	
}
