package engine.file.buffers;

import engine.exceptions.DataBaseException;
import engine.file.blocks.Block;
import engine.file.blocks.ReadableBlock;
import engine.file.streams.ReadByteStream;
import engine.file.streams.WriteByteStream;

import java.nio.ByteBuffer;

public class NoBlockBuffer extends BlockBuffer {

	public NoBlockBuffer() {
	}

	@Override
	public Block getBlockIfExistInBuffer(int num) {
		return null;
	}

	@Override
	public void hintBlock(int num) {
		if(stream==null)return;
	}

	@Override
	public void forceBlock(int num) {
		if(stream==null)return;
	}

	@Override
	public void clearBuffer() {
	}

	@Override
	public Block readBlock(int pos)  {
		if(stream==null)throw new DataBaseException("NoBlockBuffer->readBlock","BlockStream não definido!");
		try {
			return stream.readBlock(pos);
		}catch(DataBaseException e) {
			e.addLocationToPath("NoBlockBuffer");
			throw e;
		}
	}

	@Override
	public void writeBlock(int pos, Block b)  {
		if(stream==null)throw new DataBaseException("NoBlockBuffer->writeBlock","BlockStream não definido!");
		try {
			stream.writeBlock(pos,b);
		}catch(DataBaseException e) {
			e.addLocationToPath("NoBlockBuffer");
			throw e;
		}
	}

	@Override
	public void flush()  {
		stream.flush();
	}

	@Override
	public void close() {
		stream.close();
	}

	@Override
	public int getBlockSize() {
		return stream.getBlockSize();
	}

	@Override
	public ReadableBlock getBlockReadByteStream(int block)  {
		return stream.getBlockReadByteStream(block);
	}

	@Override
	public WriteByteStream getBlockWriteByteStream(int block)  {
		return stream.getBlockWriteByteStream(block);
	}

	@Override
	public int lastBlock()  {
		return stream.lastBlock();
	}

	@Override
	public void readBlock(int pos, ByteBuffer buffer)  {
		stream.readBlock(pos, buffer);
	}


}
