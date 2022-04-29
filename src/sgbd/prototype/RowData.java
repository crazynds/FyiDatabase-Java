package sgbd.prototype;

import sgbd.util.UtilConversor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RowData implements Iterable<Map.Entry<String,byte[]>> {
	private Map<String,byte[]> data;

	public RowData() {
		data=new HashMap<String, byte[]>();
	}


	public void setData(String column,byte[] data) {
		valid=false;
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
	public byte[] unset(String column){
		valid=false;
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
	public String getString(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return UtilConversor.byteArrayToString(data);
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
}
