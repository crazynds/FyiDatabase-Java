package sgbd.prototype;

import sgbd.util.statics.UtilConversor;

public class BData {
    private byte[] data;

    public BData(byte[] data){
        this.data = data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
    public void setInt(int data) {
        this.setData(UtilConversor.intToByteArray(data));
    }
    public void setLong(long data) {
        this.setData(UtilConversor.longToByteArray(data));
    }
    public void setString(String data) {
        this.setData(UtilConversor.stringToByteArray(data));
    }
    public void setFloat(float data) {
        this.setData(UtilConversor.floatToByteArray(data));
    }
    public void setDouble(double data) {
        this.setData(UtilConversor.doubleToByteArray(data));
    }
    public void setBoolean(boolean data) {
        this.setData(new byte[]{(byte) (data ? 1 : 0)});
    }

    public byte[] getData() {
        return this.data;
    }
    public Integer getInt() {
        byte[] data = this.data;
        if(data==null)return null;
        return UtilConversor.byteArrayToInt(data);
    }
    public Long getLong() {
        byte[] data = this.data;
        if(data==null)return null;
        return UtilConversor.byteArrayToLong(data);
    }
    public Float getFloat() {
        byte[] data = this.data;
        if(data==null)return null;
        return UtilConversor.byteArrayToFloat(data);
    }
    public Double getDouble() {
        byte[] data = this.data;
        if(data==null)return null;
        return UtilConversor.byteArrayToDouble(data);
    }
    public String getString() {
        byte[] data = this.data;
        if(data==null)return null;
        return UtilConversor.byteArrayToString(data);
    }
    public Boolean getBoolean() {
        byte[] data = this.data;
        if(data==null)return null;
        return data[0]!=0;
    }

    public int length(){
        return this.data.length;
    }

}
