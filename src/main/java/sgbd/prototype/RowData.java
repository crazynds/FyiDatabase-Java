package sgbd.prototype;

import sgbd.util.statics.UtilConversor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RowData implements Iterable<Map.Entry<String,byte[]>>,Comparable<RowData> {
	private Map<String,byte[]> data;
	private byte checkSum = 0;

	public RowData() {
		data=new HashMap<String, byte[]>();
	}
	protected RowData(RowData cloneData) {
		data=new HashMap<String, byte[]>(cloneData.data);
	}

	private void removeCheckSum(String column){
		byte[] arr = data.get(column);
		if(arr != null && arr.length>0){
			checkSum ^= arr[0];
		}
	}

	private void addToCheckSum(String column,byte[] data){
		if(data!= null && data.length>0){
			checkSum ^= data[0];
		}
	}


	public void setData(String column,byte[] data) {
		valid=false;
		removeCheckSum(column);
		addToCheckSum(column,data);
		this.data.put(column, data);
	}
	public void setInt(String column,int data) {
		this.setData(column, UtilConversor.intToByteArray(data));
	}
	public void setLong(String column,long data) {
		this.setData(column, UtilConversor.longToByteArray(data));
	}
	public void setString(String column,String data) {
		this.setData(column, UtilConversor.stringToByteArray(data));
	}
	public void setFloat(String column,float data) {
		this.setData(column, UtilConversor.floatToByteArray(data));
	}
	public void setDouble(String column,double data) {
		this.setData(column, UtilConversor.doubleToByteArray(data));
	}
	public void setBoolean(String column,boolean data) {
		this.setData(column, new byte[]{(byte) (data ? 1 : 0)});
	}
	public byte[] unset(String column){
		valid=false;
		removeCheckSum(column);
		return this.data.remove(column);
	}

	public byte[] getData(String column) {
		return this.data.get(column);
	}
	public Integer getInt(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return UtilConversor.byteArrayToInt(data);
	}
	public Long getLong(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return UtilConversor.byteArrayToLong(data);
	}
	public Float getFloat(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return UtilConversor.byteArrayToFloat(data);
	}
	public Double getDouble(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return UtilConversor.byteArrayToDouble(data);
	}
	public String getString(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return UtilConversor.byteArrayToString(data);
	}
	public Boolean getBoolean(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return data[0]!=0;
	}
	
	public int size() {
		return this.data.size();
	}

	private boolean valid = false;

	protected boolean isValid(){
		return valid;
	}
	protected void setValid(){
		valid=true;
	}

	@Override
	public Iterator<Map.Entry<String, byte[]>> iterator() {
		return data.entrySet().iterator();
	}

	@Override
	public int compareTo(RowData r) {
		int val = checkSum - r.checkSum;
		if(val!=0)return val;
		val = data.size() - r.data.size();
		if(val!=0)return val;
		for (Map.Entry<String, byte[]> entry:
			 this) {
			byte[] arr = r.getData(entry.getKey());
			val = Arrays.compare(arr,entry.getValue());
			if(val!= 0)return val;
		}
		return val;
	}
}
