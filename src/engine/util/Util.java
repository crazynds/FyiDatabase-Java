package engine.util;

import java.math.BigInteger;

public class Util {

	public static BigInteger convertByteArrayToNumber(byte[] arr) {
    	arr = invertByteArray(arr);
    	return new BigInteger(arr);
	}

	public static byte[] convertNumberToByteArray(BigInteger number,int size) {
    	byte[] arr = number.toByteArray();
    	arr = invertByteArray(arr);
    	byte[] aux = new byte[size];
    	for(int x=0;x<aux.length && x<arr.length;x++) {
    		aux[x]=arr[x];
    	}
    	return aux;
	}
	public static byte[] convertLongToByteArray(long num,int size) {
		byte[] arr = new byte[size];
		for(int x=0;x<size&&num>0;x++,num>>=8)
			arr[x] = (byte)num;
		return arr;
	}
	
	public static BigInteger convertToPrimaryKey(byte[] arr) {
		return convertByteArrayToNumber(arr);
	}
	
	public final static byte[] invertByteArray(byte[] array) {
		byte tmp;
		for(int x=0;x<array.length/2;x++) {
			tmp=array[x];
			array[x]=array[array.length-x-1];
			array[array.length-x-1]=tmp;
		}
		return array;
	}
    
}
