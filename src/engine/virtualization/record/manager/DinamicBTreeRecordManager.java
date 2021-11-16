package engine.virtualization.record.manager;

import engine.exceptions.DataBaseException;
import engine.file.FileManager;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;

import java.math.BigInteger;


/*
 *
 */
public class DinamicBTreeRecordManager extends RecordManager {

	public DinamicBTreeRecordManager(FileManager fm, RecordInterface ri) {
		super(fm, ri);
	}

	@Override
	public void restart(FileManager fm) {

	}

	@Override
	public void open(FileManager fm) {

	}

	@Override
	public void flush() {

	}

	@Override
	public void close() {

	}

	@Override
	public Record read(BigInteger pk) {
		return null;
	}

	@Override
	public void read(BigInteger pk, byte[] buffer) {

	}

	@Override
	public Object write(Record r, BigInteger pk) {
		return null;
	}

	@Override
	public boolean isOrdened() {
		return false;
	}

	@Override
	public RecordStream sequencialRead() {
		return null;
	}


}
