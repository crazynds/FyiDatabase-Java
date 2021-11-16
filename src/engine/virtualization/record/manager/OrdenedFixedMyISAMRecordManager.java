package engine.virtualization.record.manager;


import java.math.BigInteger;

import engine.exceptions.DataBaseException;
import engine.file.FileManager;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.RecordManager;
/*
public class OrdenedFixedMyISAMRecordManager extends FixedMyISAMManager {

	public OrdenedFixedMyISAMRecordManager(FileManager fm, RecordInterface ri, int sizeOfEachRecord)
			 {
		super(fm, ri, sizeOfEachRecord);
	}
	
	@Override
	public synchronized long writeNew(Record r)  {
		if(getRecordInterface().isActiveRecord(r)==false)return 0;
		if(qtdOfRecords==0)return writeInPosition(r, sizeOfBytesQtdRecords);
		return writeInPositionRecursive(r,convertByteInNumber(getRecordInterface().getPrimaryKey(r)),new byte[sizeOfEachRecord],0,qtdOfRecords-1);
	}

	
	@Override
	public synchronized long write(Record r, long position)  {
		long checkPosition = position-sizeOfBytesQtdRecords;
		if(checkPosition%sizeOfEachRecord!=0)throw new DataBaseException("OrdenedFixedLinearRecordManager->write", "Posi��o passada � inv�lida. ("+position+")");
		if(checkPosition/sizeOfEachRecord>qtdOfRecords)throw new DataBaseException("OrdenedFixedLinearRecordManager->write", "Tentativa de leitura de um record em uma posi��o inv�lida");
		
		
		//Se for um record n�o v�lido, s� sobrescreve o valor antigo independente do que seja.
		if(getRecordInterface().isActiveRecord(r)==false) {
			return super.writeInPosition(r, position);
		}
		
		byte[] data = new byte[sizeOfEachRecord];
		
		read(position,data);
		Record old = new GenericRecord(data);
		
		//Se o record antigo n�o for v�lido, ent�o faz o procedimento de como se escrevesse um novo assim ele faz uso da busca binaria para encontrar a nova posi��o
		if(getRecordInterface().isActiveRecord(old)==false) {
			return super.writeNew(r);
		}

		byte[] pk = getRecordInterface().getPrimaryKey(r);
		byte[] old_pk = getRecordInterface().getPrimaryKey(old);

		getRecordInterface().setActiveRecord(old, false);
		writeInPosition(old,position);
		
		BigInteger bi_pk = convertByteInNumber(pk);
		BigInteger bi_old_pk = convertByteInNumber(old_pk);
		
		switch(bi_pk.compareTo(bi_old_pk)) {
			case 0://primary key � igual ent�o segue a vida mantem a mesma posi��o
				return super.writeInPosition(r, position);
			case -1://primary key � menor que o antigo, ent�o segue pra tras;
				return writeInPositionRecursive(r, bi_pk,data,0,(int)checkPosition/sizeOfEachRecord);
			case 1://primary key � maior que o antigo, ent�o segue pra frente;
				return writeInPositionRecursive(r, bi_pk,data,(int)checkPosition/sizeOfEachRecord,qtdOfRecords-1);
			default:
				throw new DataBaseException("OrdenedFixedLinearRecordManager->write", "Erro desconhecido, isso devia acontecer?");
		}
	}
	
	protected synchronized long writeInPositionRecursive(Record r, BigInteger pk,byte[] auxData, int l, int h)  {
		if(h<l) {
			writeRecordBefore(r, sizeOfEachRecord*(h+1)+ sizeOfBytesQtdRecords);
			return h*sizeOfEachRecord+ sizeOfBytesQtdRecords;
		}
		int m = (h-l)/2 + l ;
		long position = m*sizeOfEachRecord + sizeOfBytesQtdRecords;
		
		read(position,auxData);
		Record aux = new GenericRecord(auxData);
		
		while(getRecordInterface().isActiveRecord(aux)==false && m<h) {
			m++;
			position = m*sizeOfEachRecord + sizeOfBytesQtdRecords;
			read(position,auxData);
		}
		if(getRecordInterface().isActiveRecord(aux)==false && m==h) {
			return writeInPosition(r, position);
		}
		
		BigInteger pk_aux = convertByteInNumber(getRecordInterface().getPrimaryKey(aux));
		
		switch(pk.compareTo(pk_aux)) {
		case 0:
			return super.writeInPosition(r, position);
		case -1:
			return writeInPositionRecursive(r, pk,auxData, l, m-1);
		case 1:
			return writeInPositionRecursive(r, pk,auxData, m+1, h);
		default:
			throw new DataBaseException("OrdenedFixedLinearRecordManager->writeInPositionRecursive", "Erro desconhecido, isso devia acontecer?");
		}
	}
	
	protected synchronized void writeRecordBefore(Record r, long position)  {
		long checkPosition = position-sizeOfBytesQtdRecords;
		if(checkPosition%sizeOfEachRecord!=0)throw new DataBaseException("OrdenedFixedLinearRecordManager->writeRecordBefore", "Posi��o passada � inv�lida. ("+position+")");
		if(checkPosition/sizeOfEachRecord>qtdOfRecords)throw new DataBaseException("OrdenedFixedLinearRecordManager->writeRecordBefore", "Tentativa de leitura de um record em uma posi��o inv�lida");
	
		if(checkPosition == qtdOfRecords*sizeOfEachRecord) {
			writeInPosition(r, position);
			return;
		}
		
		int actualBlock = (int) (position/getBlockBuffer().getBlockSize());
		long actualPosition = position;
		int recordNum= (int) ((actualPosition-sizeOfBytesQtdRecords)/sizeOfEachRecord);
		Record old,updated=r;
		
		do{
			if(actualBlock <= (actualPosition/getBlockBuffer().getBlockSize())) {
				getBlockBuffer().hintBlock(++actualBlock);
			}
			
			old = read(actualPosition);
			writeInPosition(updated, actualPosition);
			if(updated.equals(r)==false)
				getRecordInterface().updeteReference(getRecordInterface().getPrimaryKey(r), actualPosition);
			
			//Se o record n�o for ativo, ent�o n�o precisa realocar ele de posi��o
			if(getRecordInterface().isActiveRecord(old)==false)return;

			updated=old;
			actualPosition+=sizeOfEachRecord;
			recordNum++;
		}while(recordNum<qtdOfRecords);
		writeInPosition(updated,actualPosition);
		
	}
	
	
	@Override
	public RecordStream sequencialRead() {
		RecordManager recordManager = this;
		return new RecordStream() {
			
			byte start = sizeOfBytesQtdRecords;
			long pointer = sizeOfBytesQtdRecords;
			int lastBlock = 0;
			
			
			RecordManager rm = recordManager;
			
			Record buffer =null;
			
			
			@Override
			public void setPointer(long position) {
				pointer = position;
				lastBlock = (int) (pointer/rm.getBlockBuffer().getBlockSize());
				rm.getBlockBuffer().hintBlock(lastBlock);
			}
			
			@Override
			public void open()  {
				pointer = start;
				lastBlock = 0;
				rm.getBlockBuffer().hintBlock(lastBlock);
			}
			
			@Override
			public void close()  {
			}
			
			@Override
			public void reset() {
				pointer = start;
				lastBlock = 0;
				rm.getBlockBuffer().hintBlock(lastBlock);
			}
			
			@Override
			public Record next()  {
				buffer = rm.read(pointer);
				pointer+=sizeOfEachRecord;
				
				if(pointer/rm.getBlockBuffer().getBlockSize()>=lastBlock) {
					rm.getBlockBuffer().hintBlock(++lastBlock);
				}
				
				return buffer;
			}
			
			@Override
			public boolean isOrdened() {
				return false;
			}
			
			@Override
			public boolean hasNext()  {
				return (pointer/sizeOfEachRecord) < qtdOfRecords;
			}
			
			@Override
			public Record getRecord() {
				return buffer;
			}
			
			@Override
			public long getPosition() {
				return pointer;
			}
			@Override
			public long write(Record r)  {
				return rm.write(r, pointer);
			}
			
		};
	}

}

 */
