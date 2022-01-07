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

	protected final ReadWriteLock lock = new ReentrantReadWriteLock();

	protected RecordInterface recordInterface;
	protected HeapStorage heap;
	
	
	/*
	 * Tamanho de cada record fixo
	 */
	protected int sizeOfEachRecord;
	
	/*
	 * Tamanho do inteiro que ira representar quantos records estão armazenados
	 */
	protected byte sizeOfBytesQtdRecords = 4;
	
	
	/*
	 * Quantidade de records armazenados
	 */
	protected int qtdOfRecords;
	protected boolean changed;

	protected Record invalidRecord;

	public FixedRecordStorage(FileManager fm, RecordInterface ri, int sizeOfEachRecord){
		this(fm,ri,sizeOfEachRecord,16);
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
					byte[] num = Util.convertNumberToByteArray(BigInteger.valueOf(qtdOfRecords),sizeOfBytesQtdRecords);
					heap.write(0,num,0,sizeOfBytesQtdRecords);
					heap.commitWrites();
			}finally {
				lock.writeLock().unlock();
			}
		}
	}

	@Override
	public boolean search(BigInteger pk, byte[] buffer) {
		lock.readLock().lock();
		try {
			long startPos;
			GenericRecord r = new GenericRecord(buffer);
			startPos = findRecordBinarySearch(pk, 0, qtdOfRecords - 1, r);

			checkKey(startPos);
			heap.read(startPos,sizeOfEachRecord,buffer,0);


			return recordInterface.getPrimaryKey(r).compareTo(pk)==0;
		} finally {
			lock.readLock().unlock();
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
			TreeMap<BigInteger,byte[]> arr = new TreeMap<>();
			arr.put(pk,r.getData());
			GenericRecord buffer =new GenericRecord(new byte[sizeOfEachRecord]);

			lock.writeLock().lock();
			try {
				position = writeNewP1(arr,buffer);
				WriteByteStream wbs = getWriteByteStream();
				long writeOffset = writeNewP2(arr,buffer,wbs,position);
				writeNewP3(arr,buffer,wbs,writeOffset);
				changed=true;
				flush();
			} finally {
				lock.writeLock().unlock();
			}
		}
		return position;
	}

	private long writeNewP1(TreeMap<BigInteger,byte[]> records, GenericRecord buffer){
		long startPos;
		startPos = findRecordBinarySearch(records.firstKey(), 0, qtdOfRecords - 1, buffer);

		if(startPos > sizeOfBytesQtdRecords) {
			heap.read(startPos, sizeOfEachRecord, buffer.getData(), 0);
			while(recordInterface.isActiveRecord(buffer)==false && startPos>sizeOfBytesQtdRecords){
				startPos-=sizeOfEachRecord;
				heap.read(startPos, sizeOfEachRecord, buffer.getData(), 0);
			}
		}

		return (startPos-sizeOfBytesQtdRecords)/sizeOfEachRecord;
	}

	private long writeNewP2(TreeMap<BigInteger,byte[]> records, GenericRecord buffer,WriteByteStream wbs,long pos){
		Map.Entry<BigInteger,byte[]> entry=null;
		LinkedList<byte[]> list = new LinkedList<>();
		LinkedList<BigInteger> listKey = new LinkedList<>();
		byte[] data = null;
		long readOffset= pos;
		long writeOffset = pos;

		while(readOffset<qtdOfRecords && !records.isEmpty()){
			long readPosition = getPositionOfRecord(readOffset);
			heap.read(readPosition,sizeOfEachRecord,buffer.getData(),0);

			if(recordInterface.isActiveRecord(buffer)) {
				long writePosition = getPositionOfRecord(writeOffset);
				BigInteger firstKey = records.firstKey();
				BigInteger buffPk = recordInterface.getPrimaryKey(buffer);
				switch (firstKey.compareTo(buffPk)) {
					case -1:
						do{
							entry = records.pollFirstEntry();
							data = entry.getValue();
							wbs.write(writePosition, data, sizeOfEachRecord);
							recordInterface.updeteReference(entry.getKey(), writePosition);
							writeOffset++;
							writePosition+= sizeOfEachRecord;
							/**
							 * Estrategia de otimização:
							 * Remover a verificaçõa writeOffset <= readOffset
							 * Enquanto a key alvo for menor do que a key na fila, vai escrevendo.
							 */
						}while (writeOffset <= readOffset && !records.isEmpty() && records.firstKey().compareTo(buffPk) == -1);
						if(writeOffset>readOffset) {
							records.putIfAbsent(buffPk, buffer.getData());
							buffer.setData(data);
						}else if(writeOffset<readOffset) {
							writePosition = getPositionOfRecord(writeOffset);
							while (readOffset - writeOffset > records.size()) {
								wbs.write(writePosition, invalidRecord.getData(), invalidRecord.size());
								writeOffset++;
								writePosition+=sizeOfEachRecord;
							}
							data = buffer.getData();
							wbs.write(writePosition, data, sizeOfEachRecord);
							recordInterface.updeteReference(buffPk, writePosition);
							writeOffset++;
						}else{
							writeOffset++;
						}
						break;
					case 0:
						entry = records.pollFirstEntry();
						data = entry.getValue();
						wbs.write(writePosition, data, sizeOfEachRecord);
						recordInterface.updeteReference(entry.getKey(), writePosition);
						writeOffset++;
						break;
					case 1:
						if(writeOffset<readOffset) {
							writePosition = getPositionOfRecord(writeOffset);
							while (readOffset - writeOffset > records.size()) {
								wbs.write(writePosition, invalidRecord.getData(), invalidRecord.size());
								writeOffset++;
								writePosition+=sizeOfEachRecord;
							}
							data = buffer.getData();
							wbs.write(writePosition, data, sizeOfEachRecord);
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
		return writeOffset;
	}

	private void writeNewP3(TreeMap<BigInteger,byte[]> records, GenericRecord buffer,WriteByteStream wbs,long writeOffset){
		Map.Entry<BigInteger,byte[]> entry=null;
		byte[] data = null;
		while((entry = records.pollFirstEntry())!=null){
			long position = getPositionOfRecord(writeOffset);
			data = entry.getValue();
			wbs.write(position, data, sizeOfEachRecord);
			recordInterface.updeteReference(entry.getKey(), position);
			if(writeOffset>=qtdOfRecords)
				qtdOfRecords++;
			writeOffset++;
		}
	}


	@Override
	public void writeNew(List<Record> list) {
		TreeMap<BigInteger,byte[]> records = new TreeMap<BigInteger,byte[]>();
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
			records.put(recordInterface.getPrimaryKey(r),data);
		}

		lock.writeLock().lock();
		try {
			pos = writeNewP1(records,buffer);
			WriteByteStream wbs = getWriteByteStream();
			long writeOffset = writeNewP2(records,buffer,wbs,pos);
			writeNewP3(records,buffer,wbs,writeOffset);
			changed=true;
			flush();
		} finally {
			lock.writeLock().unlock();
		}
	}

	protected long findRecordBinarySearch(BigInteger pk, long min, long max,GenericRecord buffer){
		if(max>min){
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
					return getPositionOfRecord(mid+offset-1);
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
			public void open(boolean lockToWrite) {
				if(lockToWrite)
					lock.writeLock().lock();
				else
					lock.readLock().lock();
				pos = 0;
			}

			@Override
			public void close() {
				try {
					lock.writeLock().unlock();
				}catch (IllegalMonitorStateException e){}
				try {
					lock.readLock().unlock();
				}catch (IllegalMonitorStateException e){}
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
					return new GenericRecord(buffer);
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
			public void write(Record r) {
				boolean b =lock.writeLock().tryLock();
				if(!b)throw new DataBaseException("RecordStream->write","Não foi possivel adiquirir write lock");
				try {
					if(pos<=0)return;
					fixedRecordStorage.write(r, getPositionOfRecord(pos-1));
				}finally {
					lock.writeLock().unlock();
				}
			}

			@Override
			public void remove() {
				boolean b =lock.writeLock().tryLock();
				if(!b)throw new DataBaseException("RecordStream->write","Não foi possivel adiquirir write lock");
				try {
					if(pos<=0)return;
					GenericRecord r = new GenericRecord(buffer);
					recordInterface.setActiveRecord(r,false);
					fixedRecordStorage.write(r, getPositionOfRecord(pos-1));
				}finally {
					lock.writeLock().unlock();
				}
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
			public void setPointer(BigInteger pk) {
				long position = findRecordBinarySearch(pk,0,qtdOfRecords-1,new GenericRecord(new byte[sizeOfEachRecord]));
				checkKey(position);
				pos = (position-sizeOfBytesQtdRecords)/sizeOfEachRecord;
			}

			@Override
			public long getPointer() {
				return (pos-1)*sizeOfEachRecord+sizeOfBytesQtdRecords;
			}
		};
	}

	protected long getPositionOfRecord(long record){
		return record*sizeOfEachRecord+sizeOfBytesQtdRecords;
	}

	private long checkKey(long position) {
		long checkPosition = position-sizeOfBytesQtdRecords;
		if(checkPosition%sizeOfEachRecord!=0)throw new DataBaseException("OrdenedFixedLinearRecordManager->isValidKey", "Posição passada é inválida. ("+position+")");
		if(checkPosition/sizeOfEachRecord>qtdOfRecords)throw new DataBaseException("OrdenedFixedLinearRecordManager->isValidKey", "Posição passada é acima da quantiade de registros existentes. ("+position+")");
		return checkPosition;
	}

	protected WriteByteStream getWriteByteStream(){
		return heap;
	}
	protected ReadByteStream getReadByteStream(){
		return heap;
	}
	
}
