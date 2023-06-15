package sgbd.query;

import sgbd.prototype.BData;
import sgbd.prototype.ComplexRowData;
import sgbd.prototype.RowData;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Tuple implements Iterable<Map.Entry<String,ComplexRowData>>,Comparable<Tuple>{

    private static ComplexRowData emptyRowData = new ComplexRowData();

    // users -> dados da linha
    // cidades -> dados da linha
    // users.id
    // cidades.id
    // users2.id
    private HashMap<String, ComplexRowData> sources;

    public Tuple(){
        sources = new HashMap<>();
        //<Table, Dados>
    }

    public Tuple(Tuple left, Tuple right){
        sources = new HashMap<>();
        for (Map.Entry<String, ComplexRowData> entry:
                left) {
            this.setContent(entry.getKey(),entry.getValue().clone());
        }
        for (Map.Entry<String, ComplexRowData> entry:
                right) {
            this.setContent(entry.getKey(),entry.getValue().clone());
        }
    }

    public void setContent(String asName,ComplexRowData data){
        ComplexRowData row = sources.get(asName);
        if(row!=null){
            for (Map.Entry<String, BData> column:
                    data) {
                row.setBData(column.getKey(), column.getValue(),row.getMeta(column.getKey()));
            }
        }else{
            sources.put(asName,data);
        }
    }

    public int compareTo(Tuple t){
        for(Map.Entry<String,ComplexRowData> rowData: this){
            RowData row = t.getContent(rowData.getKey());
            int val = row.compareTo(rowData.getValue());
            if(val!=0)return val;
        }
        return 0;
    }

    public ComplexRowData getContent(String name){
        ComplexRowData rd=sources.get(name);
        if(rd == null){
            rd=new ComplexRowData();
            sources.put(name,rd);
        }
        return rd;
    }

    public Tuple clone(){
        Tuple t = new Tuple();
        for (Map.Entry<String, ComplexRowData> source:
             sources.entrySet()) {
            t.setContent(source.getKey(),source.getValue().clone());
        }
        return t;
    }

    public Iterable<String> getSources(){
        return sources.keySet();
    }

    @Override
    public Iterator<Map.Entry<String, ComplexRowData>> iterator() {
        return sources.entrySet().iterator();
    }

    public int byteSize(){
        int size = 0;
        for (Map.Entry<String,ComplexRowData> row:
            sources.entrySet()) {
            for (Map.Entry<String,BData> data:
                 row.getValue()) {
                size+=data.getValue().length();
            }
        }
        return size;
    }
}
