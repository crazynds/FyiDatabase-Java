package engine.virtualization.record.manager;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.Stack;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.imageio.stream.IIOByteBuffer;

import engine.exceptions.DataBaseException;
import engine.file.Block;
import engine.file.FileManager;
import engine.file.streams.BlockStream;
import engine.file.streams.ReadByteStream;
import engine.file.streams.WriteBlockStream;
import engine.file.streams.WriteByteStream;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
/*
public class OrdenedDynamicCustomRercordManager extends RecordManager {

	protected ReadWriteLock lock = new ReentrantReadWriteLock();
	
	
	private int maxSizeOfRecord,qtdOfBytesInSizeVar;

	private int firstPage,lastPage;
	private int qtdPages;

	public OrdenedDynamicCustomRercordManager(FileManager fm, RecordInterface ri, int maxSizeOfRecord) {
		super(fm, ri);
		if(maxSizeOfRecord>=fm.getBlockSize()-16) {
			throw new DataBaseException("OrdenedDynamicCustomRercordManager->Constructor", "Tamanho m�ximo de cada record � maior que o que cabe em um bloco.");
		}
		this.maxSizeOfRecord=maxSizeOfRecord;
		if(maxSizeOfRecord<(1<<8)-1) {
			qtdOfBytesInSizeVar=1;
		}else if(maxSizeOfRecord<(1<<16)-1) {
			qtdOfBytesInSizeVar=2;
		}else{
			qtdOfBytesInSizeVar=4;
		}
	}

	@Override
	public void restartFileSet()  {
		
		getFileManager().clearFile();
		
		firstPage = lastPage = 1;
		qtdPages = 1;
		
		Block headers = new Block(getBlockBuffer().getBlockSize());
		headers.setPointer(0);
		headers.writeSeq(convertNumberInByte(BigInteger.valueOf(maxSizeOfRecord),4), 0, 4);
		headers.writeSeq(convertNumberInByte(BigInteger.valueOf(qtdPages),4), 0, 4);
		headers.writeSeq(convertNumberInByte(BigInteger.valueOf(firstPage),4), 0, 4);
		headers.writeSeq(convertNumberInByte(BigInteger.valueOf(lastPage),4), 0, 4);
		getBlockBuffer().writeBlock(0, headers);
		
		Block firstBlock = createZeroBlock(getBlockBuffer().getBlockSize(),0,0);
		getBlockBuffer().writeBlock(1, firstBlock);
		
	}

	@Override
	public void flush()  {

	}

	@Override
	public Record read(long position)  {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void read(long position, byte[] val)  {
		// TODO Auto-generated method stub

	}

	@Override
	public long write(Record r, long position)  {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long writeNew(Record r)  {
		if(r.dataSize()>maxSizeOfRecord)throw new DataBaseException("OrdenedDynamicCustomRercordManager->writeNew","Record passado � maior que o limite estipulado");
		if(getRecordInterface().isActiveRecord(r)==false)return 0;
		int page = lastPage;
		BigInteger pk = convertByteInNumber(getRecordInterface().getPrimaryKey(r));
		
		//acha qual deveria ser a p�gina correta a se colocar o record
		boolean find = false;
		PageManager pm = null;
		do {
			if(pm==null)pm= new PageManager(page);
			else pm.changePage(page);
			while(pm.qtdOfRecords()==0 && pm.hasLastBlock()) {
				pm.changePage(pm.getLastBlockID());
			}
			if(pm.hasLastBlock()==false) {
				find = true;
				page = pm.getActualBlockID();
			}else switch(pm.compareFirstRecord(pk)) {
			case 1:
				page = pm.getLastBlockID();
				break;
			case 0:
			case -1:
				find = true;
				page = pm.getActualBlockID();
				break;
			}
		}while(find==false);
		
		pm.writeRecord(r,pk);
		
		
		return page;
	}

	@Override
	public RecordStream sequencialRead() {
		return new RecordStream() {
			
			@Override
			public long write(Record r)  {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public void setPointer(long position) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void reset()  {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void open()  {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public Record next()  {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public boolean isOrdened() {
				return true;
			}
			
			@Override
			public boolean hasNext()  {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public Record getRecord() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public long getPosition() {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public void close()  {
				// TODO Auto-generated method stub
				
			}
		};
	}
	

	
	private static Block createZeroBlock(int blockSize,int last, int next)  {
		Block b = new Block(blockSize);
		b.setPointer(0);
		b.writeSeq(convertNumberInByte(BigInteger.valueOf(last),4), 0, 4);
		b.writeSeq(convertNumberInByte(BigInteger.valueOf(next),4), 0, 4);
		b.writeSeq(convertNumberInByte(BigInteger.valueOf(0),4), 0, 4);
		return b;
	}
	
	private class PageManager{
		private ReadByteStream rbs;
		private int lastBlock;
		private int actualBlock;
		private int nextBlock;
		
		private int qtdRecords;
		
		
		public PageManager(int page)  {
			if(getFileManager().lastBlock()<page)throw new DataBaseException("OrdenedDynamicCustomRercordManager:PageManager->Constructor","Carregado p�gina que n�o existe no arquivo");
			changePage(page);
		}
		
		public void changePage(int page)  {
			actualBlock=page;
			rbs = getFileManager().getBlockReadByteStream(page);
			rbs.setPointer(0);
			byte[] inteiro = new byte[4];
			rbs.readSeq(4,inteiro, 0);
			this.lastBlock = convertByteInNumber(inteiro).intValue();
			rbs.readSeq(4,inteiro, 0);
			this.nextBlock = convertByteInNumber(inteiro).intValue();
			rbs.readSeq(4,inteiro, 0);
			this.qtdRecords = convertByteInNumber(inteiro).intValue();
		}
		
		private void resetPointer() {
			rbs.setPointer(4+4+4);
		}
		
		public boolean hasNextBlock() {
			return nextBlock>0;
		}
		
		public boolean hasLastBlock() {
			return lastBlock>0;
		}

		public int getNextBlockID() {
			return nextBlock;
		}
		public int getActualBlockID() {
			return actualBlock;
		}
		public int getLastBlockID() {
			return lastBlock;
		}
		
		public int qtdOfRecords() {
			return qtdRecords;
		}
		
		public int writeRecord(Record r,BigInteger pk){
			LinkedList<Record> list =new LinkedList<Record>();
			list.add(r);
			return writeRecord(list, pk);
		}
		
		private int writeRecord(LinkedList<Record> list,BigInteger pk){
			resetPointer();
			int record=0,size,position=rbs.getPointer(),x;
			int newQtdRecords;
			byte[] tempData = new byte[maxSizeOfRecord];
			Record r2;
			while(record<qtdRecords) {
				size = convertByteInNumber(rbs.readSeq(qtdOfBytesInSizeVar)).intValue();
				rbs.readSeq(size, tempData,0);
				r2 = new GenericRecord(tempData, size);
				switch(convertByteInNumber(getRecordInterface().getPrimaryKey(r2)).compareTo(pk)) {
				case 1://se for maior ent�o escreve antes dele e sobe ele pra cima
					list.addLast(r2);
				case 0://se for igual substitui
					//Le os records do arquivo e armazena em memoria temporaria
					for(x=1;record<qtdRecords;x++,record++) {
						size = convertByteInNumber(rbs.readSeq(qtdOfBytesInSizeVar)).intValue();
						Record r3 = new GenericRecord(new byte[size]);
						rbs.readSeq(size, r3.getData(),0);
						list.addLast(r3);
					}
					//Substrai da quantidade records a quantidade que foi lida
					qtdRecords -= x;
					break;
				case -1://se for menor ent�o segue em frente
				default:
					record++;
					position = rbs.getPointer();
					break;
				}
				
			}
			
			
			WriteByteStream wbs = getFileManager().getBlockWriteByteStream(actualBlock);
			
			wbs.setPointer(position);
			
			newQtdRecords = 0;
			for(Record auxRecord = list.pollFirst(); list.size()>0 && auxRecord.dataSize()+wbs.getPointer()<=getFileManager().getBlockSize() ; auxRecord = list.pollFirst()) {
				wbs.writeSeq(convertNumberInByte(BigInteger.valueOf(auxRecord.dataSize()),qtdOfBytesInSizeVar), 0, qtdOfBytesInSizeVar);
				wbs.writeSeq(auxRecord.getData(), 0, auxRecord.dataSize());
				newQtdRecords++;
			}
			wbs.write(8,convertNumberInByte(BigInteger.valueOf(qtdRecords+newQtdRecords), 4), 0, 4);
			if(list.size()>0) {
				if(hasNextBlock()) {
				}else {
					Block b = createZeroBlock(getFileManager().getBlockSize(), getActualBlockID(), 0);
					nextBlock = getFileManager().createNewBlock(b);
					PageManager nextPage = new PageManager(nextBlock);
					nextPage.writeRecord(list, convertByteInNumber(getRecordInterface().getPrimaryKey(list.getFirst())));
				}
			}
			saveInformations(wbs);
			wbs.commitWrites();
			
			return position;
		}
		
		private void saveInformations(WriteByteStream wbs)  {
			wbs.setPointer(0);
			ByteBuffer bb = ByteBuffer.allocate(12);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			bb.putInt(lastBlock);
			bb.putInt(nextBlock);
			bb.putInt(qtdRecords);
			wbs.writeSeq(bb.array(), 0, 12);
		}
		
		public int compareFirstRecord(BigInteger pk)  {
			if(qtdRecords==0) return 0;
			resetPointer();
			int size = convertByteInNumber(rbs.readSeq(qtdOfBytesInSizeVar)).intValue();
			Record r = new GenericRecord(rbs.readSeq(size));
			BigInteger bi = convertByteInNumber(getRecordInterface().getPrimaryKey(r));
			return bi.compareTo(pk);
		}
		
		public int compareLastRecord(BigInteger pk)  {
			if(qtdRecords==0) return 0;
			resetPointer();
			int record = 0,size;
			while(record<qtdRecords-1) {
				size = convertByteInNumber(rbs.readSeq(qtdOfBytesInSizeVar)).intValue();
				rbs.setPointer(rbs.getPointer()+size);
				record++;
			}
			size = convertByteInNumber(rbs.readSeq(qtdOfBytesInSizeVar)).intValue();
			Record r = new GenericRecord(rbs.readSeq(size));
			BigInteger bi = convertByteInNumber(getRecordInterface().getPrimaryKey(r));
			return bi.compareTo(pk);
		}
		
		
	}

}
*/