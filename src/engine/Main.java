package engine;


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.info.Parameters;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.FixedRecordManager;
import engine.virtualization.record.manager.RecordManager;

public class Main {

	public static class AuxRecordInterface implements RecordInterface{

		@Override
		public BigInteger getPrimaryKey(Record r)  {
			byte[] inteiro = new byte[4];
			byte[] data = r.getData();
			for(int x=0;x<4;x++) {
				inteiro[3-x] = data[x+1];
			}
			return new BigInteger(inteiro);
		}

		@Override
		public boolean isActiveRecord(Record r) {
			return (r.getData()[0]&0x1)!=0;
		}
		
		@Override
		public void updeteReference(BigInteger pk, long key) {
			//map.put(pk,key);
		}

		@Override
		public void setActiveRecord(Record r, boolean active) {
			byte[] arr = r.getData();
			arr[0] = (byte)( (arr[0]&(~0x1)) | ((active)?0x1:0x0));
		}
		
	}

	public static void createBase(RecordInterface ri,RecordManager rm,int sizeOfRecord,int qtdOfRecords,int sizePerList, int maxPk){
		Random rand = new Random(100);
		ArrayList<Record> list = new ArrayList<>();
		rm.restart();

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
			rm.write(list);
			System.out.println("x->"+x);
		}
		rm.flush();

	}
	
	
	public static void main(String[] args) {
		Long time = System.nanoTime();
		Record r;

		int sizeOfRecord = 300;
		int qtdOfRecords = 4000;
		int qtdPerList = 100;
		int maxPK = 100000000;

		FileManager f = new FileManager("E:\\teste.dat", new OptimizedFIFOBlockBuffer(16));
		RecordInterface ri = new AuxRecordInterface();
		RecordManager rm = new FixedRecordManager(f,ri,sizeOfRecord);


		createBase(ri,rm,sizeOfRecord,qtdOfRecords,qtdPerList,maxPK);


		RecordStream rs = rm.sequencialRead();
		rs.open();
		rs.setPointer(BigInteger.valueOf(100000));

		ArrayList<BigInteger> selecionadoRandom = new ArrayList<>();
		long x=0;
		int oldPk = -1;
		long oldPos = rs.getPointer();
		Random rand = new Random();

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

			//System.out.println("("+(x++)+"|"+ri.isActiveRecord(r)+") ->"+position+" - [PK: "+aux+" Data: "+r.getData()[5]+","+r.getData()[6]+","+r.getData()[7]+"]");
			if(x%1==0){
				selecionadoRandom.add(BigInteger.valueOf(aux));
			}
			if(x%23==0) {
				ri.setActiveRecord(r, false);
				rs.write(r);
			}
		}

		r = new GenericRecord(new byte[sizeOfRecord]);
		x = 0;
		for (BigInteger pk:
			 selecionadoRandom) {
			rm.read(pk,r.getData());
			BigInteger pk2 = ri.getPrimaryKey(r);
			if(pk.compareTo(pk2)!=0 || ri.isActiveRecord(r)==false)
				System.out.println("("+(x++)+"|"+ri.isActiveRecord(r)+") -> [PK: "+pk.intValue()+" = "+pk2.intValue()+", Data: "+r.getData()[5]+","+r.getData()[6]+","+r.getData()[7]+"]");
		}

		rs.close();
		f.close();

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
		System.out.println("Finalizou o arquivo!");

	}

}
