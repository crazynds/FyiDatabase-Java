package lib;

import engine.util.Util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BigKey implements Comparable<BigKey>{

    private final byte[] data;

    private BigInteger key = null;


    public BigKey(byte[] arr){
        this(arr,false);
    }
    public BigKey(byte[] arr,boolean clone){
        this.data = (clone)?arr.clone():arr;
    }
    public BigInteger toBigInteger(){
        if(key==null){
            key = Util.convertByteArrayToNumber(this.data);
        }
        return key;
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

        return this.toBigInteger().compareTo(o.toBigInteger());

//        int x=Math.max(this.data.length,o.data.length)-1;
//        for(;x>=o.data.length;x--){
//            if(this.data[x]!=0)return 1;
//        }
//        for(;x>=this.data.length;x--){
//            if(o.data[x]!=0)return -1;
//        }
//        for(;x>=0;x--){
//            int a = Byte.compare(this.data[x],o.data[x]);
//            if(a!=0)return a;
//        }
//        return 0;
//        return java.nio.ByteBuffer.wrap(this.getData()).compareTo(java.nio.ByteBuffer.wrap(o.data));
        //return Arrays.compare(this.data,o.data);
    }

    public static BigKey valueOf(BigInteger bi,int size){
        return new BigKey(Util.convertNumberToByteArray(bi,size));
    }

    public static BigKey valueOf(long val,int size){
        return new BigKey(Util.convertLongToByteArray(val,size));
    }
}
