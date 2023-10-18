package sgbd.prototype;

import com.sun.source.tree.Tree;
import engine.util.Util;
import lib.BigKey;
import sgbd.prototype.column.Column;
import sgbd.prototype.column.IntegerColumn;
import sgbd.prototype.metadata.*;
import sgbd.prototype.query.fields.BinaryField;
import sgbd.prototype.query.fields.BooleanField;
import sgbd.prototype.query.fields.DoubleField;
import sgbd.prototype.query.fields.Field;
import sgbd.util.global.UtilConversor;

import java.util.*;

public class RowData implements Iterable<Map.Entry<String,Field>>,Comparable<RowData> {
	private Map<String, Field> data;
	private Map<String, Column> metadata;
	private byte checkSum = 0;
	private long byteSize = 0;

	public RowData() {
		data=new TreeMap<>();
		metadata=new HashMap<>();
	}
	protected RowData(RowData cloneData) {
		data=new TreeMap<>(cloneData.data);
		metadata=new HashMap<>(cloneData.metadata);
		byteSize = cloneData.byteSize;
	}
	private void applyChecksum(BData data){
		if(data != null && data.length()>0){
			checkSum ^= data.getData()[0];
		}
	}

	public void setField(String column,Field field){
		valid = false;
		Field currentField = this.data.get(column);
		if(currentField != null){
			applyChecksum(currentField.getBData());
			byteSize -= currentField.bufferByteSize();
		}
		if(field == null)return;
		this.data.put(column,field);
		applyChecksum(field.getBData());
		byteSize += field.bufferByteSize();
	}
	public void setField(String column,Field field,Column metadata){
		setField(column,field);
		setMetadata(column,metadata);
	}
	public void setData(String column,byte[] data){
		BData bdata = new BData(data);
		this.setField(column, new BinaryField(new Metadata((short) (data.length>>8 + 1),Metadata.LSHIFT_8_SIZE_COLUMN),bdata));
	}
	public void setInt(String column,int data) {
		BData bdata = new BData(UtilConversor.intToByteArray(data));
		this.setField(column, Field.createField(IntegerMetadata.generic,bdata));
	}
	public void setLong(String column,long data) {
		BData bdata = new BData(UtilConversor.longToByteArray(data));
		this.setField(column, Field.createField(LongMetadata.generic,bdata));
	}
	public void setBigKey(String column, BigKey data) {
		BData bdata = new BData(data.getData());
		this.setField(column, Field.createField(new BigIntegerMetadata(bdata.length()),bdata));
	}
	public void setString(String column,String data) {
		BData bdata = new BData(UtilConversor.stringToByteArray(data));
		this.setField(column, Field.createField(new StringMetadata((short)(data.length()+1)),bdata));
	}
	public void setFloat(String column,float data) {
		BData bdata = new BData(UtilConversor.floatToByteArray(data));
		this.setField(column, Field.createField(FloatMetadata.generic,bdata));
	}
	public void setDouble(String column,double data) {
		BData bdata = new BData(UtilConversor.doubleToByteArray(data));
		this.setField(column, Field.createField(DoubleMetadata.generic,bdata));
	}
	public void setBoolean(String column,boolean data) {
		BData bdata = new BData(new byte[]{(byte) (data ? 1 : 0)});
		this.setField(column, Field.createField(BooleanMetadata.generic,bdata));
	}

	public void setInt(String column, int data, Column meta) {
		BData bdata = new BData(UtilConversor.intToByteArray(data));
		this.setField(column, Field.createField(meta,bdata));
		this.setMetadata(column,meta);
	}
	public void setLong(String column,Long data,Column meta) {
		BData bdata = new BData(UtilConversor.longToByteArray(data));
		this.setField(column, Field.createField(meta,bdata));
		this.setMetadata(column,meta);
	}
	public void setString(String column,String data,Column meta) {
		BData bdata = new BData(UtilConversor.stringToByteArray(data));
		this.setField(column, Field.createField(meta,bdata));
		this.setMetadata(column,meta);
	}
	public void setFloat(String column,float data,Column meta) {
		BData bdata = new BData(UtilConversor.floatToByteArray(data));
		this.setField(column, Field.createField(meta,bdata));
		this.setMetadata(column,meta);
	}
	public void setDouble(String column,double data,Column meta) {
		BData bdata = new BData(UtilConversor.doubleToByteArray(data));
		this.setField(column, Field.createField(meta,bdata));
		this.setMetadata(column,meta);
	}
	public void setBoolean(String column,boolean data,Column meta) {
		BData bdata = new BData(new byte[]{(byte) (data ? 1 : 0)});
		this.setField(column, Field.createField(meta,bdata));
		this.setMetadata(column,meta);
	}

	public Field unset(String column){
		Field f = this.data.get(column);
		setField(column,null);
		setMetadata(column,null);
		this.data.remove(column);
		return f;
	}

	public Column getMetadata(String column){
		return metadata.get(column);
	}

	public void setMetadata(String column,Column meta) {
		this.metadata.put(column,meta);
	}

	public Field getField(String column) {
		return this.data.get(column);
	}
	public byte[] getData(String column) {
		if(!this.data.containsKey(column))return null;
		BData data = this.data.get(column).getBData();
		if(data==null)return null;
		return data.getData();
	}
	public BData getBData(String column) {
		if(!this.data.containsKey(column))return null;
		BData data = this.data.get(column).getBData();
		return data;
	}
	public Integer getInt(String column) {
		if(!this.data.containsKey(column))return null;
		return this.data.get(column).getInt();
	}
	public Long getLong(String column) {
		if(!this.data.containsKey(column))return null;
		return this.data.get(column).getLong();
	}
	public BigKey getBigKey(String column) {
		if(!this.data.containsKey(column))return null;
		return this.data.get(column).getBData().getBigKey();
	}
	public Float getFloat(String column) {
		if(!this.data.containsKey(column))return null;
		return this.data.get(column).getFloat();
	}
	public Double getDouble(String column) {
		if(!this.data.containsKey(column))return null;
		return this.data.get(column).getDouble();
	}
	public String getString(String column) {
		if(!this.data.containsKey(column))return null;
		return this.data.get(column).getString();
	}
	public Boolean getBoolean(String column) {
		if(!this.data.containsKey(column))return null;
		return this.data.get(column).getBoolean();
	}

	public int size() {
		return this.data.size();
	}

	public long getByteSize() {
		return this.byteSize;
	}

	private boolean valid = false;

	protected boolean isValid(){
		return valid;
	}
	protected void setValid(){
		valid=true;
	}

	@Override
	public Iterator<Map.Entry<String, Field>> iterator() {
		return data.entrySet().iterator();
	}

	@Override
	public int compareTo(RowData r) {
		int val = checkSum - r.checkSum;
		if(val!=0)return val;
		val = data.size() - r.data.size();
		if(val!=0)return val;
		for (Map.Entry<String, Field> entry:
			 this) {
			Field f = r.getField(entry.getKey());
			val = entry.getValue().compareTo(f);
			if(val!= 0)return val;
		}
		return val;
	}
	public RowData clone(){
		return new RowData(this);
	}
}
