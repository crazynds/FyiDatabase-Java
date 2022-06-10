package engine.file.blocks;

import engine.exceptions.DataBaseException;

public class BlockID extends Block {

	private int blockId;

	public BlockID(byte[] data,int id) {
		super(data);
		this.blockId=id;
	}

	public BlockID(Block b,int id) {
		super(b,false);
		this.blockId=id;
	}
	
	
	public int getBlockId() {
		return blockId;
	}

	public void changeBlockID(byte[] data,int id) {
		if(getBlockSize()!=data.length)throw new DataBaseException("ChangeBlockID","Dados enviados possuem tamanho diferentes do necessário para reutilizar o bloco.");
		write(0, data, 0, data.length);
		this.blockId=id;
	}
	public void changeBlockID(Block b,int id) {
		if(!this.compareBlockFaces(b))throw new DataBaseException("ChangeBlockID","Dados enviados possuem tamanho diferentes do necessário para reutilizar o bloco.");
		this.data=b.getData();
		this.blockId=id;
	}
	public void changeBlockID(int id){
		this.blockId=id;
	}
	
}
