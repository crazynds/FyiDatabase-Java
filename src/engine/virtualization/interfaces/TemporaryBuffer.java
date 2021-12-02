package engine.virtualization.interfaces;

import java.util.TreeMap;
import java.util.Map.Entry;

import engine.file.Block;
import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.file.streams.ReadByteStream;
import engine.file.streams.WriteByteStream;

public class TemporaryBuffer{

	public FileManager origin;
	
	public FileManager buffer;
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
		buffer = new FileManager(origin.getNameFile()+".temp",new OptimizedFIFOBlockBuffer(blockBufferSize));
		buffer.clearFile();
		buffer.changeBlockSize(origin.getBlockSize());
		bufferedBlocks = new TreeMap<Integer, Integer>();
		usedBlocks = new TreeMap<Integer, Boolean>();
	}
	
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
		Block b = new Block(buffer.getBlockSize());
		Entry<Integer, Integer> entry;
		/*
		Fazer leitura de conjuntos de blocos e ordenar esse conjuntos e escrever de forma sequencial esse conjunto
		 */


		while ((entry = bufferedBlocks.pollFirstEntry()) != null) {
			buffer.readBlock(entry.getValue(), b.getData());
			origin.writeBlock(entry.getKey(), b);
			if (entry.getValue() < minimalAvaliable)
				minimalAvaliable = entry.getValue();
		}
		usedBlocks.clear();
		//buffer.clearFile();
	}

	public synchronized void clear(){
		origin.flush();
		buffer.clearFile();
		bufferedBlocks.clear();
		usedBlocks.clear();
		minimalAvaliable=0;
	}

}
