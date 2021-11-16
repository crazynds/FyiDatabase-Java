package engine.table.prototype;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import engine.info.Parameters;

public class ByteArray {

	ArrayList<Byte> bytes;
	
	public ByteArray() {
		bytes=new ArrayList<Byte>();
	}
	
	public int size() {
		return bytes.size();
	}
	
	public void write(Byte[] data) {
		bytes.addAll(Arrays.<Byte>asList(data));
	}


	public void write(byte[] array) {
		Byte[] aux= new Byte[array.length];
		Parameters.MEMORY_ALLOCATED_BY_BYTE_ARRAY+=aux.length;
		for(int x=0;x<aux.length;x++)
			aux[x]=array[x];
		write(aux);
	}
	
	public void writeDinamic(byte[] array) {
		Byte[] aux= new Byte[array.length];
		Parameters.MEMORY_ALLOCATED_BY_BYTE_ARRAY+=aux.length;
		for(int x=0;x<aux.length;x++)
			aux[x]=array[x];
		writeDinamic(aux);
	}
	public void writeDinamic(Byte[] data) {
		byte type;
		int tam=data.length;
		if(tam<=252){
			type=(byte) tam;
			bytes.add(type);
			write(data);//1 byte -> -3 byte que necessário
		}else if(tam<=(1<<8)-1){
			type=(byte) 253;
			bytes.add(type);
			type=((byte)tam);
			bytes.add(type);
			write(data);//2 byte -> -2 byte que necessário
		}else if(tam<=(1<<16)-1){
			type=(byte) 254;
			bytes.add(type);
			write(ByteBuffer.allocate(2).putShort((short)tam).array());
			write(data);//3 byte -> -1 byte que necessário
		}else{
			type=(byte) 255;
			bytes.add(type);
			write(ByteBuffer.allocate(4).putInt( tam).array());
			write(data);//5 bytes -> +1 byte que necessário
		}
	}
	
	public byte[] asArray() {
		Byte[] aux= bytes.toArray(new Byte[bytes.size()]);
		byte[] b = new byte[bytes.size()];
		Parameters.MEMORY_ALLOCATED_BY_BYTE_ARRAY+=b.length+aux.length;
		for(int x=0;x<b.length;x++)
			b[x]=aux[x].byteValue();
		return b;
	}


	

}
