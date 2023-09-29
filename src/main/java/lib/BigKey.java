package lib;

import engine.util.Util;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;

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
        int result = 0;
        if(this.data.length==o.data.length){
            for(int x=this.data.length-1;x>=0 && result==0;x--){
                // TODO: Arrumar comparação
                result = this.data[x]-o.data[x];
            }
        }else{
            int a = this.data.length;
            int b = o.data.length;
            int x=Integer.max(a,b)-1;
            while(x>=0 && result == 0){
                result = ((a>x)?this.data[x]:0)-((b>x)?o.data[x]:0);
                x-=1;
            }

        }
        return (result<0)?-1:(result>0)?1:0;
    }

    public static BigKey valueOf(BigInteger bi,int size){
        return new BigKey(Util.convertNumberToByteArray(bi,size));
    }

    public static BigKey valueOf(long val,int size){
        return new BigKey(Util.convertLongToByteArray(val,size));
    }
}
