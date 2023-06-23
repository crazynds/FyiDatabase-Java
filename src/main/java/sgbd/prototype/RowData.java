package sgbd.prototype;

import sgbd.util.global.UtilConversor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RowData implements Iterable<Map.Entry<String,BData>>,Comparable<RowData> {
	private Map<String,BData> data;
	private byte checkSum = 0;

	public RowData() {
		data=new HashMap<String, BData>();
	}
	protected RowData(RowData cloneData) {
		data=new HashMap<String, BData>(cloneData.data);
	}

	private void removeCheckSum(String column){
		BData arr = data.get(column);
		if(arr != null && arr.length()>0){
			checkSum ^= arr.getData()[0];
		}
	}

	private void addToCheckSum(String column,BData data){
		if(data!= null && data.length()>0){
			checkSum ^= data.getData()[0];
		}
	}


	public void setData(String column,byte[] data) {
		valid=false;
		removeCheckSum(column);
		BData newData = new BData(data);
		addToCheckSum(column,newData);
		this.data.put(column, newData);
	}
	public void setBData(String column,BData newData) {
		valid=false;
		removeCheckSum(column);
		addToCheckSum(column,newData);
		this.data.put(column, newData);
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
		if(!this.data.containsKey(column))
			return null;
		return this.data.remove(column).getData();
	}

	public byte[] getData(String column) {
		BData data = this.data.get(column);
		if(data==null)return null;
		return data.getData();
	}
	public BData getBData(String column) {
		BData data = this.data.get(column);
		if(data==null)return null;
		return data;
	}
	public Integer getInt(String column) {
		BData data = this.data.get(column);
		if(data==null)return null;
		return data.getInt();
	}
	public Long getLong(String column) {
		BData data = this.data.get(column);
		if(data==null)return null;
		return data.getLong();
	}
	public Float getFloat(String column) {
		BData data = this.data.get(column);
		if(data==null)return null;
		return data.getFloat();
	}
	public Double getDouble(String column) {
		BData data = this.data.get(column);
		if(data==null)return null;
		return data.getDouble();
	}
	public String getString(String column) {
		BData data = this.data.get(column);
		if(data==null)return null;
		return data.getString();
	}
	public Boolean getBoolean(String column) {
		BData data = this.data.get(column);
		if(data==null)return null;
		return data.getBoolean();
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
	public Iterator<Map.Entry<String, BData>> iterator() {
		return data.entrySet().iterator();
	}

	@Override
	public int compareTo(RowData r) {
		int val = checkSum - r.checkSum;
		if(val!=0)return val;
		val = data.size() - r.data.size();
		if(val!=0)return val;
		for (Map.Entry<String, BData> entry:
			 this) {
			byte[] arr = r.getData(entry.getKey());
			val = Arrays.compare(arr,entry.getValue().getData());
			if(val!= 0)return val;
		}
		return val;
	}
}
