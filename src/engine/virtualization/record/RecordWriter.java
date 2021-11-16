package engine.virtualization.record;

import engine.exceptions.DataBaseException;
import engine.file.streams.WriteByteStream;

public abstract class RecordWriter extends Record implements WriteByteStream{

		
	@Override
	public int write(long pos, byte[] data, int offset, int len) throws DataBaseException {
		return 0;
	}

	@Override
	public int writeSeq(byte[] data, int offset, int len) throws DataBaseException {
		return 0;
	}
	
	@Override
	public int write(long pos, byte[] data, int len) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void commitWrites() {
	}
}
