package engine;


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import engine.file.FileManager;
import engine.file.buffers.FIFOBlockBuffer;
import engine.file.streams.ReadByteStream;
import engine.info.Parameters;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.FixedRecordManager;
import engine.virtualization.record.manager.MemoryBTreeRecordManager;
import engine.virtualization.record.manager.RecordManager;
import sgbd.util.UtilConversor;

public class Main {

	public static class AuxRecordInterface extends RecordInterface {

		public AuxRecordInterface() {
			super(new RecordInfoExtractor() {

				private byte[] buff = new byte[4];

				@Override
				public BigInteger getPrimaryKey(ByteBuffer rbs) {
					return null;
				}

				@Override
				public synchronized BigInteger getPrimaryKey(ReadByteStream rbs) {
					rbs.read(1,buff,0,4);
					return BigInteger.valueOf(UtilConversor.byteArrayToInt(buff));
				}

				@Override
				public boolean isActiveRecord(ByteBuffer rbs) {
					return false;
				}

				@Override
				public synchronized boolean isActiveRecord(ReadByteStream rbs) {
					rbs.read(0,buff,0,1);
					return (buff[0]&0x1)!=0;
				}

				@Override
				public void setActiveRecord(Record r, boolean active) {
					byte[] arr = r.getData();
					arr[0] = (byte)( (arr[0]&(~0x1)) | ((active)?0x1:0x0));
				}
			});
		}

		@Override
		public void updeteReference(BigInteger pk, long key) {}
	}

	public static void printRecords(RecordManager rm,int sizeOfRecord){
		RecordStream rs = rm.sequencialRead();
		Record r;
		long x=0;
		long lastPos = 0;
		int lastPk = -1;
		rs.open(false);
		while(rs.hasNext()){
			r = rs.next();

			ByteBuffer wrapped = ByteBuffer.wrap(r.getData(),1,4);
			wrapped.order(ByteOrder.LITTLE_ENDIAN);
			int num = wrapped.getInt();
			if(lastPk>=num){
				System.out.println("(WARNING) Ordem PK invalida -> "+num+" <= "+lastPk);
			}
			byte dat = r.getData()[5];
			for(int z=6;z<r.size();z++){
				if(r.getData()[z]!=dat){
					System.out.println("(WARNING) Dados inválidos -> "+num+" <= arr["+z+"] == "+r.getData()[z]+", esperado => DATA=["+r.getData()[5]+", "+r.getData()[6]+"]");
					break;
				}
			}
			System.out.println("("+(x++)+") -> "+rs.getPointer()+" - [ PK="+num+", DATA=["+r.getData()[5]+", "+r.getData()[6]+"] ]");
			lastPk = num;
		}
		rs.close();
	}

	public static void createBase(RecordInterface ri,RecordManager rm,int sizeOfRecord,int qtdOfRecords,int sizePerList, int maxPk){
		Random rand = new Random(100);
		ArrayList<Record> list = new ArrayList<>();
		rm.restart();

		for(int x=0;x<qtdOfRecords;x+=sizePerList) {
			if(sizePerList!=1)
				list.clear();
			for (int y = x; y < x+sizePerList && y<qtdOfRecords; y++) {
				byte[] data = new byte[sizeOfRecord];
				Arrays.fill(data,(byte)y);

				Record r1 = new GenericRecord(data);
				//ri.setActiveRecord(r1, true);

				int val = rand.nextInt(maxPk);
				ByteBuffer b = ByteBuffer.allocate(4);
				b.order(ByteOrder.LITTLE_ENDIAN);
				b.putInt(val);
				byte[] pk = b.array();
				System.arraycopy(pk,0,data,1,4);

				ri.getExtractor().setActiveRecord(r1, true);
				if(sizePerList!=1)
					list.add(r1);
				else{
					rm.write(r1);
					System.out.println("x("+x+") = "+val);
				}
			}
			if(sizePerList!=1) {
				rm.write(list);
				System.out.println("x->" + x);
			}
		}
		rm.flush();

	}
	
	
	public static void main(String[] args) {
		Long time = System.nanoTime();

		int sizeOfRecord = 600;
		int qtdOfRecords = 10000;
		int qtdPerList = 10;
		int maxPK = 5000000;

		RecordInterface ri = new AuxRecordInterface();
		RecordManager rm = new MemoryBTreeRecordManager(ri.getExtractor());

		//FileManager f = new FileManager("bin/teste.dat", new FIFOBlockBuffer(2));
		//RecordManager rm = new FixedRecordManager(f,ri,sizeOfRecord);

		createBase(ri,rm,sizeOfRecord,qtdOfRecords,qtdPerList,maxPK);
		printRecords(rm,sizeOfRecord);

		rm.close();

		System.out.println("Tempo total: "+(System.nanoTime()-time)/1000000f+"ms");
		System.out.println("Tempo seek escrita: "+(Parameters.IO_SEEK_WRITE_TIME)/1000000f+"ms");
		System.out.println("Tempo escrita: "+(Parameters.IO_WRITE_TIME)/1000000f+"ms");
		System.out.println("Tempo seek leitura: "+(Parameters.IO_SEEK_READ_TIME)/1000000f+"ms");
		System.out.println("Tempo leitura: "+(Parameters.IO_READ_TIME)/1000000f+"ms");
		System.out.println("Tempo de sync: "+(Parameters.IO_SYNC_TIME)/1000000f+"ms");
		System.out.println("Tempo total IO: "+(Parameters.IO_SYNC_TIME
				+Parameters.IO_SEEK_WRITE_TIME
				+Parameters.IO_READ_TIME
				+Parameters.IO_SEEK_READ_TIME
				+Parameters.IO_WRITE_TIME)/1000000f+"ms");
		System.out.println("Blocos carregados: "+Parameters.BLOCK_LOADED);
		System.out.println("Blocos salvos: "+Parameters.BLOCK_SAVED);
		System.out.println("Memoria usada para blocos: "+Parameters.MEMORY_ALLOCATED_BY_BLOCKS);

	}

}
