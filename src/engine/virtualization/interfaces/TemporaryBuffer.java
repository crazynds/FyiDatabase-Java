package engine.virtualization.interfaces;

import java.util.TreeMap;
import java.util.Map.Entry;

import com.sun.source.tree.Tree;
import engine.file.Block;
import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.file.streams.ReadByteStream;
import engine.file.streams.WriteByteStream;

public class TemporaryBuffer{

	private FileManager origin;

	private FileManager buffer;
	//Block -> virtual Block
	private TreeMap<Integer, Integer> bufferedBlocks;
	private TreeMap<Integer, Boolean> usedBlocks;

	private int minimalAvaliable = 0;
	/*
		3 -> 101
		4 -> 120
		..
		0 -> cabe�alho 512
		513 -> cebe�alho 512
	 */

	@SuppressWarnings("deprecation")
	@Override
	protected void finalize() throws Throwable {
		buffer.clearFile();
		buffer.close();
		super.finalize();
	}
	
	public TemporaryBuffer(FileManager origin) {
		this(origin,16);
	}
	public TemporaryBuffer(FileManager origin, int blockBufferSize) {
		this.origin=origin;
		buffer = new FileManager(origin.getNameFile()+".temp",new OptimizedFIFOBlockBuffer(blockBufferSize));
		buffer.clearFile();
		buffer.changeBlockSize(origin.getBlockSize());
		bufferedBlocks = new TreeMap<Integer, Integer>();
		usedBlocks = new TreeMap<Integer, Boolean>();
	}

	private int lastLoaded = -1;
	
	private synchronized int loadBlockInVirtualBlock(int block) {
		Integer virtualBlock = bufferedBlocks.get(block);

		//Pequeno previsor do proximo bloco a ser buscado
		if(lastLoaded!=block){
			//Identificar o padr�o e dar hint no bloco de maneira rapida
			if(lastLoaded+1 == block){
				//Se o lastLoaded = x e o bloco atual q ele qr � x+1, ent�o existe grande chance dele querer o x+2
				buffer.getBuffer().hintBlock(block+1);
			}else if(lastLoaded-1==block){
				//Se o lastLoaded = x e o bloco atual q ele qr � x-1, ent�o existe grande chance dele querer o x-2
				if(block-1>=0)
					buffer.getBuffer().hintBlock(block-1);
			}
			lastLoaded = block;
		}
		if(virtualBlock==null) {
			while(usedBlocks.get(minimalAvaliable)!=null) {
				minimalAvaliable++;
			}
			virtualBlock = minimalAvaliable++;
			buffer.writeBlock(virtualBlock,origin.readBlock(block));
			bufferedBlocks.put(block, virtualBlock);
			usedBlocks.put(virtualBlock,true);
		}
		return virtualBlock;
	}

	public WriteByteStream getBlockWriteByteStream(int block) {
		int virtualBlock = loadBlockInVirtualBlock(block);
		return buffer.getBlockWriteByteStream(virtualBlock);
	}

	public ReadByteStream getBlockReadByteStream(int block) {
		return origin.getBlockReadByteStream(block);
	}

	public synchronized void commit() {
//		Block b = new Block(buffer.getBlockSize());
		Entry<Integer, Integer> entry;
		/*
		Fazer leitura de conjuntos de blocos e ordenar esse conjuntos e escrever de forma sequencial esse conjunto
		 */

		final int blockSizeHint = 32;
		final int blockSize = (blockSizeHint < bufferedBlocks.size()>>2)?blockSizeHint:((bufferedBlocks.size()>>1)+1);

		Block[] arrBuffer = new Block[blockSize];
		for (int x=0;x<arrBuffer.length;x++) {
			arrBuffer[x] = new Block(buffer.getBlockSize());
		}
		TreeMap<Integer,Block> treeMap = new TreeMap<>();
		while (!bufferedBlocks.isEmpty()) {
			treeMap.clear();
			/**
			 * Carrega N blocos e depois salva esses N blocos em ordem;
			 */
			while ((entry = bufferedBlocks.pollFirstEntry()) != null && treeMap.size()<blockSize) {
				buffer.readBlock(entry.getValue(),arrBuffer[treeMap.size()].getData());
				treeMap.put(entry.getKey(),arrBuffer[treeMap.size()]);
				if (entry.getValue() < minimalAvaliable)
					minimalAvaliable = entry.getValue();
			}
			Entry<Integer,Block> entry2;
			while((entry2 = treeMap.pollFirstEntry())!=null){
				origin.writeBlock(entry2.getKey(),entry2.getValue());
			}
			/*
			buffer.readBlock(entry.getValue(), b.getData());
			origin.writeBlock(entry.getKey(), b);
			if (entry.getValue() < minimalAvaliable)
				minimalAvaliable = entry.getValue();
				*/

		}
		usedBlocks.clear();
	}

	public synchronized void clear(){
		origin.flush();
		buffer.clearFile();
		bufferedBlocks.clear();
		usedBlocks.clear();
		minimalAvaliable=0;
	}

}
