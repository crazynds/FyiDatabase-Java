package sgbd.query;

import sgbd.prototype.ComplexRowData;
import sgbd.prototype.RowData;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Tuple implements Iterable<Map.Entry<String,ComplexRowData>>{

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

    public void setContent(String asName,ComplexRowData data){
        ComplexRowData row = sources.get(asName);
        if(row!=null){
            for (Map.Entry<String,byte[]> column:
                    data) {
                row.setData(column.getKey(), column.getValue(),row.getMeta(column.getKey()));
            }
        }else{
            sources.put(asName,data);
        }
    }

    public ComplexRowData getContent(String name){
        ComplexRowData rd=sources.get(name);
        if(rd == null){
            rd=new ComplexRowData();
            sources.put(name,rd);
        }
        return rd;
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
            for (Map.Entry<String,byte[]> data:
                 row.getValue()) {
                size+=data.getValue().length;
            }
        }
        return size;
    }
}
