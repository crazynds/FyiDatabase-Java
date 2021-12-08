package sgbd.prototype;

import java.util.HashMap;
import java.util.Map;

public class RowData {
	private Map<String,byte[]> data;
	
	public RowData() {
		data=new HashMap<String, byte[]>();
	}

	
	public void addData(String column,byte[] data) {
		this.data.put(column, data);
	}

	public byte[] getData(String column) {
		return this.data.get(column);
	}
	public int getDataInt(String column) {
		byte[] data = this.data.get(column);
		return 0;
	}
	public float getDataFloat(String column) {
		byte[] data = this.data.get(column);
		return 0;
	}
	public String getDataString(String column) {
		byte[] data = this.data.get(column);
		return null;
	}
	
	public int size() {
		return this.data.size();
	}

}
