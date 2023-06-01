package sgbd.prototype;

import sgbd.util.statics.UtilConversor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ComplexRowData extends RowData{

    private Map<String,Column> metaData;

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
        metaData=new HashMap<String,Column>(data.metaData);
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
    public void setInt(String column,int data,Column meta) {
        this.setData(column, UtilConversor.intToByteArray(data),meta);
    }
    public void setString(String column,String data,Column meta) {
        this.setData(column, UtilConversor.stringToByteArray(data),meta);
    }
    public void setFloat(String column,float data,Column meta) {
        this.setData(column, UtilConversor.floatToByteArray(data),meta);
    }
    public void setDouble(String column,double data,Column meta) {
        this.setData(column, UtilConversor.doubleToByteArray(data),meta);
    }
    public void setBoolean(String column,boolean data,Column meta) {
        this.setData(column, new byte[]{(byte) (data ? 1 : 0)}, meta);
    }

    public ComplexRowData clone(){
        return new ComplexRowData(this);
    }


}
