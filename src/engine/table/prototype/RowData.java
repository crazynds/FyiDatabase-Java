package engine.table.prototype;

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
	
	public int size() {
		return this.data.size();
	}

}
