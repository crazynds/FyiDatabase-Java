package engine.file;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import engine.exceptions.DataBaseException;
import engine.file.blocks.Block;
import engine.file.blocks.ReadableBlock;
import engine.file.buffers.BlockBuffer;
import engine.file.blocks.BlockFace;
import engine.file.buffers.FIFOBlockBuffer;
import engine.file.streams.BlockStream;
import engine.file.streams.WriteByteStream;
import engine.info.Parameters;

public final class FileManager implements BlockFace,BlockStream {
	private RandomAccessFile file;
	private FileChannel inChannel;
	private String nameFile;

	private File fileOriginal;
		
	private BlockBuffer buffer;
	private int blockSize;

	public FileManager(File file){
		this(file,new FIFOBlockBuffer(4));
	}

	public FileManager(String file)  {
		this(file,new FIFOBlockBuffer(4));
	}
	public FileManager(File file,BlockBuffer b)  {
		this(file.getPath(),b);
	}
	public FileManager(String file,BlockBuffer b)  {
		try {
			this.fileOriginal = new File(file);
			this.file = new RandomAccessFile(this.fileOriginal, "rw");
			this.inChannel = this.file.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		nameFile=file;
		blockSize=4096;
		buffer = b; 
		buffer.startBuffering(directAcessFile);
	}
	
	public void changeBlockSize(int blockSize) {
		this.blockSize=blockSize;
	}
	
	@Override
	public Block readBlock(int block)  {
		return buffer.readBlock(block);
	}

	@Override
	public void readBlock(int num, ByteBuffer buffer)  {
		this.buffer.readBlock(num, buffer);
	}
	
	@Override
	public void writeBlock(int block,Block b)  {
		buffer.writeBlock(block,b);
	}

	@Override
	public WriteByteStream getBlockWriteByteStream(int block)  {
		return buffer.getBlockWriteByteStream(block);
	}
	
	@Override
	public ReadableBlock getBlockReadByteStream(int block)  {
		return buffer.getBlockReadByteStream(block);
	}

	@Override
	public void close() {
		buffer.close();
		try {
			file.close();
		}catch (IOException e){
		};
	}
	
	@Override
	public void flush()  {
		buffer.flush();
	}
	
	public synchronized int createNewBlock(Block b)  {
		if(!compareBlockFaces(b))throw new DataBaseException("FileManager->createNewBlock","Bloco com tamanho diferente de arquivo de destino!");
		int size=lastBlock();
		int block;
		if(size==-1)block=0;
		else block = size;
		writeBlock(block, b);
		return block;
	}
	
	private BlockStream directAcessFile = new BlockStream() {
		
		@Override
		public void writeBlock(int pos, Block b)  {
			if(!compareBlockFaces(b))throw new DataBaseException("FileManager->writeBlock","Bloco com tamanho diferente de arquivo de destino!");
			try {
				long local = pos;
				local*=blockSize;
				long time = System.nanoTime();
				ByteBuffer buff = b.getBuffer();
				inChannel.position(local);
				Parameters.IO_SEEK_WRITE_TIME+=System.nanoTime()-time;
				buff.position(0);
				inChannel.write(buff);
				Parameters.IO_WRITE_TIME+=System.nanoTime()-time;
			} catch (IOException e) {
				throw new DataBaseException("FileManager->directAcessFile->writeBlock",e.getMessage());
			}
			Parameters.BLOCK_SAVED++;
		}

		@Override
		public Block readBlock(int pos)  {
			Block b = new Block(blockSize,true);
			try {
				long time = System.nanoTime();
				long local = pos;
				ByteBuffer buff = b.getBuffer();
				local*=blockSize;
				inChannel.position(local);
				Parameters.IO_SEEK_READ_TIME+=System.nanoTime()-time;
				buff.position(0);
				inChannel.read(buff);
				Parameters.IO_READ_TIME+=System.nanoTime()-time;

			}catch(EOFException e) {
				throw new DataBaseException("FileManager->readBlock","Fim do arquivo encontado, não possivel concluir a leitura!");
			}catch (IOException e) {
				throw new DataBaseException("FileManager->directAcessFile->readBlock",e.getMessage());
			}
			Parameters.BLOCK_LOADED++;
			return b;
		}

		@Override
		public void readBlock(int pos, ByteBuffer buffer)  {
			if(buffer.capacity()!=getBlockSize())throw new DataBaseException("FileManager->directAcessFile->readBlock","Tamanho de buffer passado inválido");
			try {
				long time = System.nanoTime();
				long local = pos;
				local*=blockSize;
				inChannel.position(local);
				Parameters.IO_SEEK_READ_TIME+=System.nanoTime()-time;
				buffer.position(0);
				inChannel.read(buffer);
				Parameters.IO_READ_TIME+=System.nanoTime()-time;
			}catch(EOFException e) {
				throw new DataBaseException("FileManager->readBlock","Fim do arquivo encontado, não possivel concluir a leitura!");
			}catch (IOException e) {
				throw new DataBaseException("FileManager->directAcessFile->readBlock",e.getMessage());
			}
			Parameters.BLOCK_LOADED++;
		}
		
		
		@Override
		public void flush()  {
			try {
				long time = System.nanoTime();
				inChannel.force(false);
				Parameters.IO_SYNC_TIME+=System.nanoTime()-time;
			}catch (IOException e) {
				System.out.println(e);
				throw new DataBaseException("FileManager->directAcessFile->readBlock",e.getMessage());
			}     
		}
		
		@Override
		public void close() {
			try {
				long time = System.nanoTime();
				inChannel.force(true);
				inChannel.close();
				Parameters.IO_SYNC_TIME+=System.nanoTime()-time;
			} catch (SyncFailedException e) {
			} catch (IOException e) {
			}
		}

		@Override
		public int getBlockSize() {
			return blockSize;
		}

		@Override
		public ReadableBlock getBlockReadByteStream(int block)  {
			Block b = readBlock(block);
			return b;
		}

		@Override
		public WriteByteStream getBlockWriteByteStream(int pos)  {
			throw new DataBaseException("FileManager->directAcessFile->getBlockWriteByteStream","Função desabilitada para acesso direto ao arquivo");
		}

		@Override
		public int lastBlock()  {
			try {
				int lastBlock = (int) (file.length()/getBlockSize());
				return lastBlock-1;
			} catch (IOException e) {
				throw new DataBaseException("FileManager->directAcessFile->lastBlock", e.getMessage());
			}
		}

	};

	public String getNameFile() {
		return nameFile;
	}

	public File getFile(){
		return fileOriginal;
	}

	@Override
	public int getBlockSize() {
		return blockSize;
	}
	
	public BlockBuffer getBuffer() {
		return buffer;
	}
	@Override
	public int lastBlock() {
		return buffer.lastBlock();
	}
	
	public void clearFile() {
		try {
			buffer.clearBuffer();
			close();
			file= null;
			this.fileOriginal.delete();
			file = new RandomAccessFile(nameFile, "rw");
			this.inChannel = this.file.getChannel();
		} catch (IOException e) {
		}
	}	
	
}
