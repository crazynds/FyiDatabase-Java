package engine.file;

import engine.file.blocks.Block;
import engine.file.blocks.ReadableBlock;
import engine.file.buffers.FIFOBlockBuffer;
import engine.file.buffers.OptimizedFIFOBlockBuffer;
import engine.file.streams.BlockStream;
import engine.info.Parameters;

import java.nio.ByteBuffer;

public class Main {


    public static void main(String[] args){
        String fileName= "W:/test";
        FileManager fileManager = new FileManager(fileName, new OptimizedFIFOBlockBuffer(4));

        long time = System.nanoTime();

        Block b = new Block(fileManager.getBlockSize());
        ByteBuffer buff = ByteBuffer.allocate(8);

        for(int x=0;x<512*512;x++){
            buff.put(0,(byte)1);
            buff.put(1,(byte)2);
            buff.put(2,(byte)3);
            buff.put(3,(byte)x);
            b.setPointer(0);
            for(int y=0;y<b.getBlockSize();y+=4){
                b.writeSeq(buff.array(),0,4);
            }
            //System.out.println("colocou no "+x);
            fileManager.writeBlock(x,b);
        }
        fileManager.flush();
        fileManager.close();

        fileManager = new FileManager(fileName, new OptimizedFIFOBlockBuffer(4));

        ReadableBlock readableBuffer;
        for(int x=0;x<512*512;x++){
            readableBuffer = fileManager.getBlockReadByteStream(x);
            readableBuffer.setPointer(0);
            buff.put(0,readableBuffer.readSeq(8),0,8);
            if(buff.get(3)+buff.get(7) != ((byte)x)*2){
                System.out.println("NOT OK Esperado"+((byte)x)+" Get "+buff.get(3)+" + "+buff.get(7));
            }
            /*System.out.print("Buffer "+x+" : [");
            for(int y=0;y<4;y++){
                System.out.print(buff.get(y)+" ");
            }
            System.out.println(']');*/
        }

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
        System.out.println("Memoria usada para blocos diretos: "+Parameters.MEMORY_ALLOCATED_BY_DIRECT_BLOCKS);
        System.out.println("Memoria usada para blocos indiretos: "+Parameters.MEMORY_ALLOCATED_BY_INDIRECT_BLOCKS);

    }
}
