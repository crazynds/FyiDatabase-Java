package sgbd.prototype;

import sgbd.util.Conversor;

import java.util.HashMap;
import java.util.Map;

public class RowData {
	private Map<String,byte[]> data;

	public RowData() {
		data=new HashMap<String, byte[]>();
	}


	public void setData(String column,byte[] data) {
		valid=false;
		this.data.put(column, data);
	}
	public void setInt(String column,int data) {
		this.setData(column, Conversor.intToByteArray(data));
	}
	public void setString(String column,String data) {
		this.setData(column, Conversor.stringToByteArray(data));
	}
	public void setFloat(String column,float data) {
		this.setData(column, Conversor.floatToByteArray(data));
	}
	public byte[] unset(String column){
		valid=false;
		return this.data.remove(column);
	}

	public byte[] getData(String column) {
		return this.data.get(column);
	}
	public Integer getDataInt(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return Conversor.byteArrayToInt(data);
	}
	public Float getDataFloat(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return Conversor.byteArrayToFloat(data);
	}
	public String getDataString(String column) {
		byte[] data = this.data.get(column);
		if(data==null)return null;
		return Conversor.byteArrayToString(data);
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

}
