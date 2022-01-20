package engine;


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import engine.file.FileManager;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.file.streams.ReadByteStream;
import engine.info.Parameters;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;
import engine.virtualization.record.manager.FixedRecordManager;
import engine.virtualization.record.manager.RecordManager;
import sgbd.util.Conversor;

public class Main {

	public static class AuxRecordInterface implements RecordInterface{

		private byte[] buff = new byte[4];

		@Override
		public synchronized BigInteger getPrimaryKey(Record r)  {
			byte[] inteiro = buff;
			byte[] data = r.getData();
			for(int x=0;x<4;x++) {
				inteiro[3-x] = data[x+1];
			}
			return new BigInteger(inteiro);
		}

		@Override
		public synchronized BigInteger getPrimaryKey(ReadByteStream rbs) {
			rbs.read(1,4,buff,0);
			return BigInteger.valueOf(Conversor.byteArrayToInt(buff));
		}

		@Override
		public boolean isActiveRecord(Record r) {
			return (r.getData()[0]&0x1)!=0;
		}

		@Override
		public synchronized boolean isActiveRecord(ReadByteStream rbs) {
			rbs.read(0,1,buff,0);
			return (buff[0]&0x1)!=0;
		}

		@Override
		public void updeteReference(BigInteger pk, long key) {}

		@Override
		public void setActiveRecord(Record r, boolean active) {
			byte[] arr = r.getData();
			arr[0] = (byte)( (arr[0]&(~0x1)) | ((active)?0x1:0x0));
		}
		
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
			if(lastPos<rs.getPointer()-sizeOfRecord){
				System.out.println("(WARNING) GAP AQ DE "+((rs.getPointer()-lastPos)/sizeOfRecord-1));
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
			lastPos = rs.getPointer();
		}
		rs.close();
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

		int sizeOfRecord = 400;
		int qtdOfRecords = 50000;
		int qtdPerList = 10000;
		int maxPK = 1000000000;

		FileManager f = new FileManager("W:\\teste.dat", new OptimizedFIFOBlockBuffer(16));
		RecordInterface ri = new AuxRecordInterface();
		RecordManager rm = new FixedRecordManager(f,ri,sizeOfRecord);
		rm.restart();
		createBase(ri,rm,sizeOfRecord,qtdOfRecords,qtdPerList,maxPK);
		rm.flush();
		printRecords(rm,sizeOfRecord);

		/*
		boolean exec = true;
		Scanner in = new Scanner(System.in);
		int qtd = 0;
		do{
			System.out.println("Escolha uma opção abaixo:");
			System.out.println("");
			System.out.println("1-Inserir");
			System.out.println("2-Remover");
			System.out.println("3-Visualizar itens");
			System.out.println("4-Resetar arquivo");
			System.out.println("5-Flush nos dados");
			System.out.println("0-Sair");

			int option = in.nextInt();

			switch(option){
				case 0:
					exec = false;
					break;
				case 1:
					int pk = 0;
					do{
						System.out.println("Digite um valor para primary key(0 para sair): ");
						System.out.println("--os dados serao preenchidos aleatoriamente-- ");
						pk = in.nextInt();

						if(pk!=0) {
							ByteBuffer b = ByteBuffer.allocate(4);
							b.order(ByteOrder.LITTLE_ENDIAN);
							b.putInt(pk);
							byte[] pkArray = b.array();
							byte[] data = new byte[sizeOfRecord];
							for (int x = 0; x < data.length; x++) data[x] = (byte) qtd;
							qtd++;
							System.arraycopy(pkArray, 0, data, 1, 4);
							r = new GenericRecord(data);

							ri.setActiveRecord(r, true);
							rm.write(r);
						}
					}while(pk!=0);
					break;
				case 2:
					do{
						System.out.println("Digite um valor para primary key(0 para sair): ");
						System.out.println("--O record sera apagado--");
						pk = in.nextInt();
						if(pk!=0) {
							try {
								r = rm.read(Util.convertIntegerToByteArray(pk));
								ri.setActiveRecord(r, false);
								rm.write(r);
							} catch (NotFoundRowException e) {
								System.out.println("Não encontrado um item com essa primary key");
							}
						}
					}while(pk!=0);
					break;
				case 3:
					RecordStream rs = rm.sequencialRead();
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
						if(lastPos<rs.getPointer()-sizeOfRecord){
							System.out.println("(WARNING) GAP AQ DE "+((rs.getPointer()-lastPos)/sizeOfRecord-1));
						}
						System.out.println("("+(x++)+") -> "+rs.getPointer()+" - [ PK="+num+", DATA=["+r.getData()[5]+", "+r.getData()[6]+"] ]");
						lastPk = num;
						lastPos = rs.getPointer();
					}
					rs.close();
					break;
				case 4:
					rm.restart();
					break;
				case 5:
					rm.flush();
					break;
			}
			System.out.print("\033[H\033[2J");
			System.out.flush();
		}while(exec);
		 */

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
