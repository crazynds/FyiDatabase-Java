package engine;


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import engine.file.FileManager;
import engine.file.buffers.FIFOBlockBuffer;
import engine.file.buffers.UpdatedFIFOBlockBuffer;
import engine.info.Parameters;
import engine.util.Util;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.storage.RecordStorageController;
import engine.virtualization.record.manager.storage.FixedRecordStorage;

public class Main {

	public static TreeMap<BigInteger,Long> map = new TreeMap<>();

	public static class AuxRecordInterface implements RecordInterface{

		@Override
		public BigInteger getPrimaryKey(Record r)  {
			byte[] inteiro = new byte[4];
			for(int x=0;x<4;x++) {
				inteiro[3-x] = r.pos(x+1);
			}
			return new BigInteger(inteiro);
		}

		@Override
		public boolean isActiveRecord(Record r) {
			return (r.pos(0)&0x1)!=0;
		}
		
		@Override
		public void updeteReference(BigInteger pk, long key) {
			//map.put(pk,key);
		}

		@Override
		public void setActiveRecord(Record r, boolean active) {
			r.set(0,(byte)( (r.pos(0)&(~0x1)) | ((active)?0x1:0x0)));
		}
		
	}

	public static void createBase(RecordInterface ri,RecordStorageController rm,int sizeOfRecord,int qtdOfRecords,int sizePerList, int maxPk){
		Random rand = new Random(100);
		Record r;
		ArrayList<Record> list = new ArrayList<>();
		rm.restartFileSet();

		for(int x=0;x<qtdOfRecords;x+=sizePerList) {
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

				ri.setActiveRecord(r1, true);
				list.add(r1);
			}
			rm.writeNew(list);
			System.out.println("x->"+x);
		}
		rm.flush();

/*
		int sizeRemove = 15;
		for(int x=0;x<sizeRemove;x++){
			int val = rand.nextInt(maxPk);
			BigInteger i = BigInteger.valueOf(val);
			r = rm.read(map.get(i));
			ri.setActiveRecord(r,false);
			rm.write(r,map.get(i));
		}
		rm.flush();
 */

	}
	
	
	public static void main(String[] args) {
		Long time = System.nanoTime();
		ArrayList<Record> list = new ArrayList<>();
		Record r;

		int sizeOfRecord = 500;
		int qtdOfRecords = 100000;
		int qtdPerList = 10000;
		int maxPK = 100000000;

		FileManager f = new FileManager("E:\\teste.dat", new UpdatedFIFOBlockBuffer(16));
		RecordInterface ri = new AuxRecordInterface();
		RecordStorageController rm = new FixedRecordStorage(f,ri,sizeOfRecord,16);


		createBase(ri,rm,sizeOfRecord,qtdOfRecords,qtdPerList,maxPK);

		/*
		int[] array2 = {
				9,13,15
		};
		list = new ArrayList<>();
		for(int y=0;y<array2.length;y++) {
			byte[] data = new byte[sizeOfRecord];
			for(int x=0;x<data.length;x++) {
				data[x]=(byte)y;
			}
			Record r1 = new GenericRecord(data);
			//ri.setActiveRecord(r1, true);

			int val = array2[y];
			//int val = rand.nextInt(10000);
			ByteBuffer b = ByteBuffer.allocate(4);
			b.order(ByteOrder.LITTLE_ENDIAN);
			b.putInt(val);
			byte[] pk = b.array();
			for(int x=0;x<4;x++){
				data[x+1]=pk[x];
			}
			ri.setActiveRecord(r1, true);
			list.add(r1);
		}
		rm.writeNew(list);
		rm.flush();
		 */


		RecordStream rs = rm.sequencialRead();
		rs.open();

		long x=0;
		int oldPk = -1;
		long oldPos = 0;

		while(rs.hasNext()) {
			r = rs.next();
			long position = rs.getPointer();

			ByteBuffer b = ByteBuffer.wrap(r.getData(), 1, 4);
			b.order(ByteOrder.LITTLE_ENDIAN);
			int aux = b.getInt();
			if(oldPos+sizeOfRecord<position){
				System.out.println("(WARNING) GAP aq de "+(position - (oldPos+sizeOfRecord))/sizeOfRecord+" !!");
			}
			if(aux<oldPk) {
				System.out.println("(WARNING) Menor aqui ("+oldPk+")<("+aux+") !!");
			}
			oldPk = aux;
			oldPos = position;

			System.out.println("("+(x++)+"|"+ri.isActiveRecord(r)+") ->"+position+" - [PK: "+aux+" Data: "+r.getData()[5]+","+r.getData()[6]+","+r.getData()[7]+"]");
		}
		rs.close();
		f.close();

		System.out.println("Tempo total: "+(System.nanoTime()-time)/1000000f+"ms");
		System.out.println("Tempo seek escrita: "+(Parameters.IO_SEEK_WRITE_TIME)/1000000f+"ms");
		System.out.println("Tempo escrita: "+(Parameters.IO_WRITE_TIME)/1000000f+"ms");
		System.out.println("Tempo seek leitura: "+(Parameters.IO_SEEK_READ_TIME)/1000000f+"ms");
		System.out.println("Tempo leitura: "+(Parameters.IO_READ_TIME)/1000000f+"ms");
		System.out.println("Tempo de sync: "+(Parameters.IO_SYNC_TIME)/1000000f+"ms");
		System.out.println("Blocos carregados: "+Parameters.BLOCK_LOADED);
		System.out.println("Blocos salvos: "+Parameters.BLOCK_SAVED);
		System.out.println("Memoria usada para blocos: "+Parameters.MEMORY_ALLOCATED_BY_BLOCKS);
		System.out.println("Finalizou o arquivo!");
		
	}

}
