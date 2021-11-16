package engine.virtualization.record.manager.storage;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import engine.exceptions.DataBaseException;
import engine.file.FileManager;
import engine.file.streams.ReadByteStream;
import engine.file.streams.WriteByteStream;
import engine.util.Util;
import engine.virtualization.interfaces.HeapStorage;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;

public class FixedRecordStorage implements RecordStorageController {

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private RecordInterface recordInterface;
	private HeapStorage heap;
	
	
	/*
	 * Tamanho de cada record fixo
	 */
	private int sizeOfEachRecord;
	
	/*
	 * Tamanho do inteiro que ira representar quantos records estão armazenados
	 */
	private byte sizeOfBytesQtdRecords = 4;
	
	
	/*
	 * Quantidade de records armazenados
	 */
	private int qtdOfRecords;
	private boolean changed;

	private Record invalidRecord;

	public FixedRecordStorage(FileManager fm, RecordInterface ri, int sizeOfEachRecord){
		this(fm,ri,sizeOfEachRecord,8);
	}

	public FixedRecordStorage(FileManager fm, RecordInterface ri, int sizeOfEachRecord,int tempBufferSize)  {
		this.recordInterface = ri;
		
		this.sizeOfEachRecord=sizeOfEachRecord;
		try {
			this.heap = new HeapStorage(fm,tempBufferSize);
		}catch(IOException e){
			throw new DataBaseException("FixedRecordStorage->Constructor","Erro ao criar heap storage. "+e.getMessage());
		}

		if(fm.lastBlock()==-1)restartFileSet();
		else {
			ReadByteStream rbs = getReadByteStream();
			byte[] arr = rbs.read(0, sizeOfBytesQtdRecords);
			qtdOfRecords = Util.convertByteArrayToNumber(arr).intValue();
		}
		changed=false;
		invalidRecord = new GenericRecord(new byte[sizeOfEachRecord]);
		recordInterface.setActiveRecord(invalidRecord,false);
	}


	@Override
	public void restartFileSet() {
		lock.writeLock().lock();
		try {
			heap.clearFile();
			qtdOfRecords = 0;
			changed = true;
			this.flush();
		}finally {
			lock.writeLock().unlock();
		}
	}

	
	@Override
	public void flush() {
		if(changed) {
			lock.writeLock().lock();
			try {
					WriteByteStream wbs = getWriteByteStream();
					byte[] num = Util.convertNumberToByteArray(BigInteger.valueOf(qtdOfRecords),sizeOfBytesQtdRecords);
					heap.write(0,num,0,sizeOfBytesQtdRecords);
					heap.commitWrites();
			}finally {
				lock.writeLock().unlock();
			}
		}
	}


	@Override
	public Record read(long key) {
		checkKey(key);
		GenericRecord record = new GenericRecord(new byte[sizeOfEachRecord]);
		heap.read(key,sizeOfEachRecord,record.getData(),0);
		return record;
	}


	@Override
	public void read(long key, byte[] buffer) {
		checkKey(key);
		heap.read(key,sizeOfEachRecord,buffer,0);
	}


	@Override
	public long write(Record r, long key) {
		checkKey(key);
		BigInteger pk = recordInterface.getPrimaryKey(r);

		lock.writeLock().lock();
		try {
			if(recordInterface.isActiveRecord(r)){
				byte[] buffer = heap.read(key,sizeOfEachRecord);
				GenericRecord buff = new GenericRecord(buffer);
				if(recordInterface.isActiveRecord(buff)){
					BigInteger pkBuff = recordInterface.getPrimaryKey(buff);

					WriteByteStream wbs = getWriteByteStream();
					if(pk.compareTo(pkBuff)==0){
						wbs.write(key,r.getData(),(r.size()>sizeOfEachRecord)?sizeOfEachRecord:r.size());
						recordInterface.updeteReference(pk,key);
					}else{
						recordInterface.setActiveRecord(buff,false);
						wbs.write(key,buff.getData(),buff.size());
						return writeNew(r);
					}
					changed=true;
					flush();
				}else {
					return writeNew(r);
				}
			}else{
				WriteByteStream wbs = getWriteByteStream();
				wbs.write(key,r.getData(),(r.size()>sizeOfEachRecord)?sizeOfEachRecord:r.size());
				recordInterface.updeteReference(pk,key);
				changed=true;
				flush();
			}
		} finally {
			lock.writeLock().unlock();
		}
		return key;
	}


	@Override
	public long writeNew(Record r) {
		if(recordInterface.isActiveRecord(r)==false)return -1;
		BigInteger pk = recordInterface.getPrimaryKey(r);
		long position = 0;
		int size = r.size();
		if(size>sizeOfEachRecord)size=sizeOfEachRecord;

		if(qtdOfRecords==0){
			lock.writeLock().lock();
			try {
				WriteByteStream wbs = getWriteByteStream();
				position = getPositionOfRecord(0);
				wbs.write(position,r.getData(),r.size());
				recordInterface.updeteReference(pk,position);
				qtdOfRecords++;
				changed=true;
				flush();
			} finally {
				lock.writeLock().unlock();
			}
		}else {
			GenericRecord buffer = new GenericRecord(new byte[sizeOfEachRecord]);
			long idealPos;

			lock.readLock().lock();
			try {
				idealPos = findRecordBinarySearch(pk, 0, qtdOfRecords - 1, buffer);
			} finally {
				lock.readLock().unlock();
			}

			lock.writeLock().lock();
			try {
				WriteByteStream wbs = getWriteByteStream();
				if(qtdOfRecords*sizeOfEachRecord+sizeOfBytesQtdRecords<=idealPos){
					position = idealPos;
					wbs.write(position, r.getData(), size);
					recordInterface.updeteReference(pk, position);
					qtdOfRecords++;
				}else {
					heap.read(idealPos, sizeOfEachRecord, buffer.getData(), 0);

					if (recordInterface.isActiveRecord(buffer) == false) {
						position = idealPos;
						wbs.write(position, r.getData(), size);
						recordInterface.updeteReference(pk, position);
					} else {
						BigInteger pk2 = recordInterface.getPrimaryKey(buffer);
						switch (pk.compareTo(pk2)) {
							case 1:
								idealPos += sizeOfEachRecord;
								heap.read(idealPos, sizeOfEachRecord, buffer.getData(), 0);
							case -1:
								position = idealPos;
								wbs.write(position, r.getData(), size);
								recordInterface.updeteReference(pk, position);

								long pos = (idealPos - sizeOfBytesQtdRecords) / sizeOfEachRecord + 1;
								while (recordInterface.isActiveRecord(buffer) && pos < qtdOfRecords) {
									idealPos = getPositionOfRecord(pos);
									heap.read(idealPos, sizeOfEachRecord, buffer.getData(), 0);

									wbs.write(idealPos, buffer.getData(), buffer.size());
									pk2 = recordInterface.getPrimaryKey(buffer);
									recordInterface.updeteReference(pk2, idealPos);
									pos++;
								}

								if (recordInterface.isActiveRecord(buffer)) {
									idealPos = getPositionOfRecord(pos);
									wbs.write(idealPos, buffer.getData(), buffer.size());
									pk2 = recordInterface.getPrimaryKey(buffer);
									recordInterface.updeteReference(pk2, idealPos);
								}
								qtdOfRecords++;
								break;
							case 0:
								heap.write(idealPos, r.getData(), size);
								recordInterface.updeteReference(pk, idealPos);
								break;
						}
						changed = true;
					}
				}
				flush();
			} finally {
				lock.writeLock().unlock();
			}
		}
		return position;
	}

	@Override
	public void writeNew(List<Record> list) {
		TreeMap<BigInteger,Record> records = new TreeMap<BigInteger,Record>();
		byte[] data = null;

		GenericRecord buffer = new GenericRecord(new byte[sizeOfEachRecord]);
		long pos = 0;

		for (Record r:list) {
			if(r.size()==sizeOfEachRecord){
				data = r.getData();
				if(data.length<sizeOfEachRecord)throw new DataBaseException("FixedRecordStorage->writeNew","Tamanho passado no vetor de dados é menor que o informado na classe record");
			}else{
				data = new byte[sizeOfEachRecord];
				System.arraycopy(r.getData(),0,data,0,(r.size()<sizeOfEachRecord)?r.size():sizeOfEachRecord);
			}
			records.put(recordInterface.getPrimaryKey(r),new GenericRecord(data));
		}

		lock.readLock().lock();
		try {
			long startPos;
			startPos = findRecordBinarySearch(records.firstKey(), 0, qtdOfRecords - 1, buffer);

			if(startPos > sizeOfBytesQtdRecords) {
				heap.read(startPos-sizeOfEachRecord, sizeOfEachRecord, buffer.getData(), 0);
				while(recordInterface.isActiveRecord(buffer)==false && startPos>sizeOfBytesQtdRecords){
					startPos-=sizeOfBytesQtdRecords;
					heap.read(startPos-sizeOfEachRecord, sizeOfEachRecord, buffer.getData(), 0);
				}
			}

			pos = (startPos-sizeOfBytesQtdRecords)/sizeOfEachRecord;
		} finally {
			lock.readLock().unlock();
		}

		lock.writeLock().lock();
		try {
			WriteByteStream wbs = getWriteByteStream();
			Map.Entry<BigInteger,Record> entry=null;
			long readOffset= pos;
			long writeOffset = pos;

			while(readOffset<qtdOfRecords && records.size()>0){
				long position = getPositionOfRecord(readOffset);
				long writePosition = getPositionOfRecord(writeOffset);
				heap.read(position,sizeOfEachRecord,buffer.getData(),0);


				if(recordInterface.isActiveRecord(buffer)) {
					BigInteger buffPk = recordInterface.getPrimaryKey(buffer);
					switch (records.firstKey().compareTo(buffPk)) {
						case -1:
							while (writeOffset <= readOffset && records.size()>0 && records.firstKey().compareTo(buffPk) == -1) {
								entry = records.pollFirstEntry();
								data = entry.getValue().getData();
								wbs.write(writePosition, data, (data.length < sizeOfEachRecord) ? data.length : sizeOfEachRecord);
								recordInterface.updeteReference(entry.getKey(), writePosition);
								writeOffset++;
								writePosition = getPositionOfRecord(writeOffset);
							}
							if(writeOffset>readOffset) {
								records.put(buffPk, buffer);
								buffer = new GenericRecord(data);
							}else if(writeOffset<readOffset) {
								while (readOffset - writeOffset > records.size()) {
									wbs.write(getPositionOfRecord(writeOffset), invalidRecord.getData(), invalidRecord.size());
									writeOffset++;
								}
								data = buffer.getData();
								writePosition = getPositionOfRecord(writeOffset);
								wbs.write(writePosition, data, (data.length < sizeOfEachRecord) ? data.length : sizeOfEachRecord);
								recordInterface.updeteReference(buffPk, writePosition);
								writeOffset++;
							}else{
								writeOffset++;
							}
							break;
						case 0:
							entry = records.pollFirstEntry();
							data = entry.getValue().getData();
							wbs.write(writePosition, data, (data.length < sizeOfEachRecord) ? data.length : sizeOfEachRecord);
							recordInterface.updeteReference(entry.getKey(), writePosition);
							writeOffset++;
							break;
						case 1:
							if(writeOffset<readOffset) {
								while (readOffset - writeOffset > records.size()) {
									wbs.write(getPositionOfRecord(writeOffset), invalidRecord.getData(), invalidRecord.size());
									writeOffset++;
								}
								data = buffer.getData();
								writePosition = getPositionOfRecord(writeOffset);
								wbs.write(writePosition, data, (data.length < sizeOfEachRecord) ? data.length : sizeOfEachRecord);
								recordInterface.updeteReference(buffPk, writePosition);
								writeOffset++;
							}else{
								writeOffset=readOffset+1;
							}
							break;
					}
				}
				readOffset++;
			}
			while((entry = records.pollFirstEntry())!=null){
				long position = getPositionOfRecord(writeOffset);
				data = entry.getValue().getData();
				wbs.write(position, data, (data.length < sizeOfEachRecord) ? data.length : sizeOfEachRecord);
				recordInterface.updeteReference(entry.getKey(), position);
				if(writeOffset>=qtdOfRecords)
					qtdOfRecords++;
				writeOffset++;
			}
			flush();
		} finally {
			lock.writeLock().unlock();
		}
	}

	private long findRecordBinarySearch(BigInteger pk, long min, long max,GenericRecord buffer){
		if(max>=min){
			long mid = min + (max - min)/2;
			long offset = 0;

			do {
				heap.read(getPositionOfRecord(mid+offset), sizeOfEachRecord, buffer.getData(), 0);
				offset++;
			}while(recordInterface.isActiveRecord(buffer)==false && max>=mid+offset);
			if(max<mid+offset && recordInterface.isActiveRecord(buffer)==false){
				return findRecordBinarySearch(pk,min,mid-1,buffer);
			}

			BigInteger pk2 = recordInterface.getPrimaryKey(buffer);

			switch (pk.compareTo(pk2)){
				case -1:
					return findRecordBinarySearch(pk,min,mid-1,buffer);
				case 0:
					return getPositionOfRecord(mid);
				case 1:
					return findRecordBinarySearch(pk,mid+offset,max,buffer);
			}

		}
		return getPositionOfRecord(min);
	}


	@Override
	public RecordStream sequencialRead() {
		FixedRecordStorage frs = this;
		return new RecordStream() {

			FixedRecordStorage fixedRecordStorage = frs;

			byte[] buffer = new byte[sizeOfEachRecord];
			byte[] buffer2 = new byte[sizeOfEachRecord];

			long pos = 0;
			Record record = null;

			@Override
			public void open() {
				lock.readLock().lock();
				pos = 0;
			}

			@Override
			public void close() {
				lock.readLock().unlock();
			}

			@Override
			public boolean hasNext() {
				if(pos>=qtdOfRecords)return false;
				boolean find = false;
				long actualPos = pos;
				GenericRecord record = new GenericRecord(buffer2);
				do {
					read(getPositionOfRecord(actualPos), buffer2);
					actualPos++;
				}while(recordInterface.isActiveRecord(record)==false && actualPos<qtdOfRecords);
				if(recordInterface.isActiveRecord(record)){
					find =true;
				}
				return find;
			}

			@Override
			public Record next() {
				if(pos>=qtdOfRecords)return null;
				GenericRecord record = new GenericRecord(buffer);
				do {
					read(getPositionOfRecord(pos), buffer);
					pos++;
				}while(recordInterface.isActiveRecord(record)==false && pos<qtdOfRecords);
				if(recordInterface.isActiveRecord(record)){
					return new GenericRecord(buffer.clone());
				}
				return null;
			}

			@Override
			public void next(byte[] buff) {
				if(pos>=qtdOfRecords)return;
				GenericRecord record = new GenericRecord(buffer);
				do {
					read(getPositionOfRecord(pos), buffer);
					pos++;
				}while(recordInterface.isActiveRecord(record)==false && pos<qtdOfRecords);
				if(recordInterface.isActiveRecord(record)){
					System.arraycopy(buffer,0,buff,0,(buff.length<buffer.length)?buff.length:buffer.length);
				}
			}

			@Override
			public Record getRecord() {
				return new GenericRecord(buffer.clone());
			}

			@Override
			public void getRecord(byte[] buff) {
				System.arraycopy(buffer,0,buff,0,(buff.length<buffer.length)?buff.length:buffer.length);
			}

			@Override
			public long write(Record r) {
				return fixedRecordStorage.write(r,pos);
			}

			@Override
			public boolean isOrdened() {
				return true;
			}

			@Override
			public void reset() {
				pos = 0;
			}

			@Override
			public void setPointer(long position) {
				checkKey(position);
				pos = (position-sizeOfBytesQtdRecords)/sizeOfEachRecord;
			}

			@Override
			public long getPointer() {
				return (pos-1)*sizeOfEachRecord+sizeOfBytesQtdRecords;
			}
		};
	}

	private long getPositionOfRecord(long record){
		long pos = record*sizeOfEachRecord+sizeOfBytesQtdRecords;
		return pos;
	}

	private long checkKey(long position) {
		long checkPosition = position-sizeOfBytesQtdRecords;
		if(checkPosition%sizeOfEachRecord!=0)throw new DataBaseException("OrdenedFixedLinearRecordManager->isValidKey", "Posição passada é inválida. ("+position+")");
		if(checkPosition/sizeOfEachRecord>qtdOfRecords)throw new DataBaseException("OrdenedFixedLinearRecordManager->isValidKey", "Posição passada é acima da quantiade de registros existentes. ("+position+")");
		return checkPosition;
	}

	private WriteByteStream getWriteByteStream(){
		return heap;
	}
	private ReadByteStream getReadByteStream(){
		return heap;
	}
	
}
