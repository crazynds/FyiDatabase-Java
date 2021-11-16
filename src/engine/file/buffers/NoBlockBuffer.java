package engine.file.buffers;

import engine.exceptions.DataBaseException;
import engine.file.Block;
import engine.file.blocks.BlockBuffer;
import engine.file.streams.BlockStream;
import engine.file.streams.ReadByteStream;
import engine.file.streams.WriteByteStream;

public class NoBlockBuffer extends BlockBuffer {
	
	private BlockStream fm;

	public NoBlockBuffer() {
	}

	@Override
	public void startBuffering(BlockStream directStream) {
		this.fm=directStream;
	}

	@Override
	public Block getBlockIfExistInBuffer(int num) {
		return null;
	}

	@Override
	public void hintBlock(int num) {
		if(fm==null)return;
		
	}

	@Override
	public void forceBlock(int num) {
		if(fm==null)return;
	}

	@Override
	public void clearBuffer() {
	}

	@Override
	public Block readBlock(int pos)  {
		if(fm==null)throw new DataBaseException("NoBlockBuffer->readBlock","BlockStream não definido!");
		try {
			return fm.readBlock(pos);
		}catch(DataBaseException e) {
			e.addLocationToPath("NoBlockBuffer");
			throw e;
		}
	}

	@Override
	public void writeBlock(int pos, Block b)  {
		if(fm==null)throw new DataBaseException("NoBlockBuffer->writeBlock","BlockStream não definido!");
		try {
			fm.writeBlock(pos,b);
		}catch(DataBaseException e) {
			e.addLocationToPath("NoBlockBuffer");
			throw e;
		}
	}

	@Override
	public void flush()  {
		fm.flush();
	}

	@Override
	public void close() {
		fm.close();
	}

	@Override
	public int getBlockSize() {
		return fm.getBlockSize();
	}

	@Override
	public ReadByteStream getBlockReadByteStream(int block)  {
		return fm.getBlockReadByteStream(block);
	}

	@Override
	public WriteByteStream getBlockWriteByteStream(int block)  {
		return fm.getBlockWriteByteStream(block);
	}

	@Override
	public int lastBlock()  {
		return fm.lastBlock();
	}

	@Override
	public void readBlock(int pos, byte[] buffer)  {
		fm.readBlock(pos, buffer);
	}


}
