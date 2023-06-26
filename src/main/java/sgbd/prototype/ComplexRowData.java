package sgbd.prototype;

import sgbd.prototype.column.Column;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ComplexRowData extends RowData{

    private Map<String, Column> metaData;

    public ComplexRowData(){
        super();
        metaData=new HashMap<String,Column>();
    }
    public ComplexRowData(Map<String,Column> metaData){
        super();
        this.metaData=metaData;
    }
    protected ComplexRowData(ComplexRowData data){
        super(data);
        metaData=data.metaData;
    }

    public void makeMetaConstant(){
        metaData = Collections.unmodifiableMap(metaData);
    }


    public Column getMeta(String column){
        return metaData.get(column);
    }

    public void setData(String column,byte[] data,Column meta) {
        setData(column,data);
        this.metaData.put(column,meta);
    }
    public void setMetaData(String column,Column meta) {
        this.metaData.put(column,meta);
    }
    public void setBData(String column,BData data,Column meta) {
        setBData(column,data);
        this.metaData.put(column,meta);
    }
    public void setInt(String column,int data,Column meta) {
        this.setInt(column,data);
        this.setMetaData(column,meta);
    }
    public void setLong(String column,Long data,Column meta) {
        this.setLong(column,data);
        this.setMetaData(column,meta);
    }
    public void setString(String column,String data,Column meta) {
        this.setString(column,data);
        this.setMetaData(column,meta);
    }
    public void setFloat(String column,float data,Column meta) {
        this.setFloat(column,data);
        this.setMetaData(column,meta);
    }
    public void setDouble(String column,double data,Column meta) {
        this.setDouble(column,data);
        this.setMetaData(column,meta);
    }
    public void setBoolean(String column,boolean data,Column meta) {
        this.setBoolean(column,data);
        this.setMetaData(column,meta);
    }

    public ComplexRowData clone(){
        return new ComplexRowData(this);
    }


}
