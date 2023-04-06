package engine.virtualization.interfaces;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

import engine.file.blocks.Block;
import engine.file.FileManager;
import engine.file.blocks.ReadableBlock;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
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
		0 -> cabeçalho 512
		513 -> cebeçalho 512
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
		try {
			File tempFile = File.createTempFile(origin.getNameFile(),".dat");
			buffer = new FileManager(tempFile,new OptimizedFIFOBlockBuffer(blockBufferSize));
			tempFile.deleteOnExit();
		} catch (IOException e) {
		}
		buffer.clearFile();
		buffer.changeBlockSize(origin.getBlockSize());
		bufferedBlocks = new TreeMap<Integer, Integer>();
		usedBlocks = new TreeMap<Integer, Boolean>();
	}

	private int lastLoaded = -1;
	
	private synchronized int loadBlockInVirtualBlock(int block) {
		Integer virtualBlock = bufferedBlocks.get(block);


		if(virtualBlock==null) {
			while(usedBlocks.get(minimalAvaliable)!=null) {
				minimalAvaliable++;
			}
			virtualBlock = minimalAvaliable++;
			buffer.writeBlock(virtualBlock,origin.readBlock(block));
			bufferedBlocks.put(block, virtualBlock);
			usedBlocks.put(virtualBlock,true);
		}
		//Pequeno previsor do proximo bloco a ser buscado
		if(lastLoaded!=block){
			//Identificar o padrão e dar hint no bloco de maneira rapida
			if(lastLoaded+1 == block){
				//Se o lastLoaded = x e o bloco atual q ele qr é x+1, então existe grande chance dele querer o x+2
				buffer.getBuffer().hintBlock(block+1);
			}else if(lastLoaded-1==block){
				//Se o lastLoaded = x e o bloco atual q ele qr é x-1, então existe grande chance dele querer o x-2
				if(block-1>=0)
					buffer.getBuffer().hintBlock(block-1);
			}
			lastLoaded = block;
		}
		return virtualBlock;
	}

	public WriteByteStream getBlockWriteByteStream(int block) {
		int virtualBlock = loadBlockInVirtualBlock(block);
		return buffer.getBlockWriteByteStream(virtualBlock);
	}

	public ReadableBlock getBlockReadByteStream(int block) {
		return origin.getBlockReadByteStream(block);
	}

	public synchronized void commit() {
//		Block b = new Block(buffer.getBlockSize());
		/*
			Fazer leitura de conjuntos de blocos e ordenar esse conjuntos e escrever de forma sequencial esse conjunto
		 */

		final int blockSizeHint = 32;
		final int blockSize = (blockSizeHint < bufferedBlocks.size()>>2)?blockSizeHint:((bufferedBlocks.size()>>1)+1);

		Block[] arrBuffer = new Block[blockSize];
		for (int x=0;x<arrBuffer.length;x++) {
			arrBuffer[x] = new Block(buffer.getBlockSize());
		}

		TreeMap<Integer,Integer> auxTreeMap = new TreeMap<>();
		for(Entry<Integer, Integer> entry: bufferedBlocks.entrySet()){
			auxTreeMap.put(entry.getValue(),entry.getKey());
		}

		Entry<Integer, Integer> entry;
		TreeMap<Integer,Block> treeMap = new TreeMap<>();
		while (!auxTreeMap.isEmpty()) {
			treeMap.clear();
			/**
			 * Carrega N blocos e depois salva esses N blocos em ordem;
			 */
			while (treeMap.size()<blockSize && (entry = auxTreeMap.pollFirstEntry()) != null) {
				buffer.readBlock(entry.getKey(),arrBuffer[treeMap.size()].getBuffer());
				treeMap.put(entry.getValue(),arrBuffer[treeMap.size()]);
				if (entry.getKey() < minimalAvaliable)
					minimalAvaliable = entry.getKey();
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
		bufferedBlocks.clear();
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
