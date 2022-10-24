package engine.virtualization.record.manager;

import engine.Main;
import engine.file.FileManager;
import engine.file.buffers.FIFOBlockBuffer;
import engine.info.Parameters;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInterface;
import engine.virtualization.record.RecordStream;
import engine.virtualization.record.instances.GenericRecord;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

public class SimpleMain {

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

    public static void main(String[] args) {
        Long time = System.nanoTime();
        Random rand = new Random(12);

        int sizeOfRecord = 600;
        int maxPk = 100;
        int qtdOfRecords = 10;

        RecordInterface ri = new Main.AuxRecordInterface();
        FileManager fm = new FileManager("bin/teste.dat", new FIFOBlockBuffer(4));
        RecordManager rm = new FixedBTreeRecordManager(fm,ri.getExtractor(),4,sizeOfRecord);

        rm.restart();
        for (int y = 0; y < qtdOfRecords; y++) {
            byte[] data = new byte[sizeOfRecord];
            Arrays.fill(data,(byte)y);

            Record r1 = new GenericRecord(data);

            int val = rand.nextInt(maxPk);
            ByteBuffer b = ByteBuffer.allocate(4);
            b.order(ByteOrder.LITTLE_ENDIAN);
            b.putInt(val);
            byte[] pk = b.array();
            System.arraycopy(pk,0,data,1,4);

            ri.getExtractor().setActiveRecord(r1, true);
            rm.write(r1);
        }
        rm.flush();

        //printRecords(rm,sizeOfRecord);



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
