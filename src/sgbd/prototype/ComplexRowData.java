package sgbd.prototype;

import sgbd.util.UtilConversor;

import java.util.HashMap;

public class ComplexRowData extends RowData{

    private HashMap<String,Column> metaData;

    public ComplexRowData(){
        super();
        metaData=new HashMap<>();
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



}
