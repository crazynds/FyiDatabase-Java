package engine.file;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;

import engine.exceptions.DataBaseException;
import engine.file.blocks.BlockBuffer;
import engine.file.blocks.BlockFace;
import engine.file.buffers.NoBlockBuffer;
import engine.file.streams.BlockStream;
import engine.file.streams.ReadByteStream;
import engine.file.streams.WriteByteStream;
import engine.info.Parameters;

public class FileManager implements BlockFace,BlockStream {
	private RandomAccessFile file;
	private String nameFile;
		
	private BlockBuffer buffer;
	private int blockSize;

	public FileManager(String file)  {
		try {
			this.file = new RandomAccessFile(file, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		nameFile=file;
		blockSize=4096;
		buffer = new NoBlockBuffer();
		buffer.startBuffering(directAcessFile);
	}
	public FileManager(String file,BlockBuffer b)  {
		try {
			this.file = new RandomAccessFile(file, "rw");
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
	public void readBlock(int num, byte[] buffer)  {
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
	public ReadByteStream getBlockReadByteStream(int block)  {
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
	
	public Block createBlockSized() {
		return new Block(blockSize);
	}
	
	private BlockStream directAcessFile = new BlockStream() {
		
		@Override
		public void writeBlock(int pos, Block b)  {
			if(!compareBlockFaces(b))throw new DataBaseException("FileManager->writeBlock","Bloco com tamanho diferente de arquivo de destino!");
			try {
				long local = pos;
				local*=blockSize;
				long time = System.nanoTime();
				file.seek(local);
				Parameters.IO_SEEK_WRITE_TIME+=System.nanoTime()-time;
				file.write(b.getData());
				Parameters.IO_WRITE_TIME+=System.nanoTime()-time;
			} catch (IOException e) {
				throw new DataBaseException("FileManager->directAcessFile->writeBlock",e.getMessage());
			}
			Parameters.BLOCK_SAVED++;
		}
		
		@Override
		public Block readBlock(int pos)  {
			byte[] data= new byte[blockSize];
			try {
				long time = System.nanoTime();
				long local = pos;
				local*=blockSize;
				file.seek(local);
				Parameters.IO_SEEK_READ_TIME+=System.nanoTime()-time;
				file.readFully(data);
				Parameters.IO_READ_TIME+=System.nanoTime()-time;
				
			}catch(EOFException e) {
				throw new DataBaseException("FileManager->readBlock","Fim do arquivo encontado, não possivel concluir a leitura!");
			}catch (IOException e) {
				throw new DataBaseException("FileManager->directAcessFile->readBlock",e.getMessage());
			}
			Parameters.BLOCK_LOADED++;
			return new Block(data);
		}

		@Override
		public void readBlock(int pos, byte[] buffer)  {
			if(buffer.length!=getBlockSize())throw new DataBaseException("FileManager->directAcessFile->readBlock","Tamanho de buffer passado inválido");
			try {
				long time = System.nanoTime();
				long local = pos;
				local*=blockSize;
				file.seek(local);
				Parameters.IO_SEEK_READ_TIME+=System.nanoTime()-time;
				file.readFully(buffer);
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
				file.getFD().sync();
				Parameters.IO_SYNC_TIME+=System.nanoTime()-time;
			}catch (IOException e) {
				throw new DataBaseException("FileManager->directAcessFile->readBlock",e.getMessage());
			}     
		}
		
		@Override
		public void close() {
			try {
				long time = System.nanoTime();
				file.getFD().sync();
				file.close();
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
		public ReadByteStream getBlockReadByteStream(int block)  {
			Block b = readBlock(block);
			return new ReadByteStream() {
				
				private Block block = b;

				@Override
				public byte[] read(long pos, int len)  {
					return block.read(pos, len);
				}

				@Override
				public byte[] readSeq(int len)  {
					return block.readSeq(len);
				}

				@Override
				public void setPointer(long pos) {
					block.setPointer(pos);
				}

				@Override
				public long getPointer() {
					return block.getPointer();
				}

				@Override
				public int read(long pos, int len, byte[] buffer,int offset)  {
					return block.read(pos, len, buffer,offset);
				}

				@Override
				public int readSeq(int len, byte[] buffer,int offset)  {
					return block.readSeq(len, buffer,offset);
				}
			};
		}

		@Override
		public WriteByteStream getBlockWriteByteStream(int pos)  {
			throw new DataBaseException("FileManager->directAcessFile->getBlockWriteByteStream","Função desabilitada para acesso direto ao arquivo");
			/*byte[] data= new byte[blockSize];
			try {
				file.seek(pos*blockSize);
				file.readFully(data);
			}catch(EOFException e) {
				throw new DataBaseException("FileManager->directAcessFile->readBlock","Fim do arquivo encontado, não possivel concluir a leitura!");
			}catch (IOException e) {
				throw new DataBaseException("FileManager->directAcessFile->readBlock",e.getMessage());
			}
			Parameters.BLOCK_LOADED++;
			CommitableBlockStream b = new CommitableBlockStream(data);
			BlockStream bs = this;
			b.setCommitCallable(new Callable<Void>() {
				private CommitableBlockStream blockCommitable= b;
				private int id = pos;
				private BlockStream blockStream = bs;


				@Override
				public Void call() throws Exception {
					blockStream.writeBlock(id, blockCommitable);
					blockStream.flush();
					return null;
				}
			});
			return (WriteByteStream) b;*/
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
			File f = new File(nameFile);
			f.delete();
			file = new RandomAccessFile(nameFile, "rw");
		} catch (IOException e) {
		}
	}	
	
}
