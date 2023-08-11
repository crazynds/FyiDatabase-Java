package sgbd.prototype.query;

import sgbd.prototype.BData;
import sgbd.prototype.RowData;
import sgbd.prototype.query.fields.Field;
import sgbd.prototype.query.fields.NullField;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Tuple implements Iterable<Map.Entry<String, RowData>>,Comparable<Tuple>{

    private static RowData emptyRowData = new RowData();

    // users -> dados da linha
    // cidades -> dados da linha
    // users.id
    // cidades.id
    // users2.id
    private HashMap<String, RowData> sources;

    public Tuple(){
        sources = new HashMap<>();
        //<Table, Dados>
    }

    public Tuple(Tuple left, Tuple right){
        sources = new HashMap<>();
        for (Map.Entry<String, RowData> entry:
                left) {
            this.setContent(entry.getKey(),entry.getValue());
        }
        for (Map.Entry<String, RowData> entry:
                right) {
            this.setContent(entry.getKey(),entry.getValue());
        }
    }

    public void setContent(String asName,RowData data){
        if(data==null){
            sources.remove(asName);
            return;
        }
        RowData row = sources.get(asName);
        if(row!=null){
            for (Map.Entry<String, Field> column:
                    data) {
                row.setField(column.getKey(), column.getValue(),row.getMetadata(column.getKey()));
            }
        }else{
            sources.put(asName,data.clone());
        }
    }

    public Field getField(String[] splited){
        if(splited.length==1)
            for (Map.Entry<String,RowData> row:
                 sources.entrySet()) {
                Field f = row.getValue().getField(splited[0]);
                if(f!=null)return f;
            }
        else{
            Field f = sources.get(splited[0]).getField(splited[1]);
            if(f!=null)return f;
        }
        return NullField.generic;
    }

    public int compareTo(Tuple t){
        for(Map.Entry<String,RowData> rowData: this){
            RowData row = t.getContent(rowData.getKey());
            int val = row.compareTo(rowData.getValue());
            if(val!=0)return val;
        }
        return 0;
    }

    public RowData getContent(String name){
        RowData rd=sources.get(name);
        if(rd == null){
            rd=new RowData();
            sources.put(name,rd);
        }
        return rd;
    }

    public Tuple clone(){
        Tuple t = new Tuple();
        for (Map.Entry<String, RowData> source:
             sources.entrySet()) {
            t.setContent(source.getKey(),source.getValue().clone());
        }
        return t;
    }

    public Iterable<String> getSources(){
        return sources.keySet();
    }

    @Override
    public Iterator<Map.Entry<String, RowData>> iterator() {
        return sources.entrySet().iterator();
    }

    public int byteSize(){
        int size = 0;
        for (Map.Entry<String,RowData> row:
            sources.entrySet()) {
            for (Map.Entry<String,Field> data:
                 row.getValue()) {
                size+=data.getValue().getBData().length();
            }
        }
        return size;
    }
}
