package lib;

import engine.util.Util;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class BigKey implements Comparable<BigKey>{

    private final byte[] data;


    public BigKey(byte[] arr){
        this(arr,false);
    }
    public BigKey(byte[] arr,boolean clone){
        this.data = (clone)?arr.clone():arr;
    }
    public BigInteger toBigInteger(){
        return Util.convertByteArrayToNumber(this.data);
    }

    public Long longValue(){
        return toBigInteger().longValue();
    }

    public byte[] getData(){
        return data;
    }



    // 0010: 2

    // 1110: -2

    // 111101101-0000000000000000000:
    // 111101101-1111111111111111111:


    // "olaz"   => [.,.,x,y]    y>x
    // "olb"    => [.,.,x+1,0]



    @Override
    public int compareTo(BigKey o) {

//        ByteBuffer wrapped = ByteBuffer.wrap(o.getData()); // big-endian by default
//
//        int num1 = java.nio.ByteBuffer.wrap(this.data).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
//        int num2 = java.nio.ByteBuffer.wrap(o.getData()).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();

        return o.toBigInteger().compareTo(this.toBigInteger());
    }

    public static BigKey valueOf(BigInteger bi,int size){
        return new BigKey(Util.convertNumberToByteArray(bi,size));
    }

    public static BigKey valueOf(long val,int size){
        return new BigKey(Util.convertLongToByteArray(val,size));
    }
}
