package sgbd.query.basic;

import sgbd.prototype.RowData;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Tuple implements Iterable<Map.Entry<String,RowData>>{

    private HashMap<String, RowData> sources;

    public Tuple(){
        sources = new HashMap<>();
        //<Table, Dados>
    }

    public void setContent(String asName,RowData data){
        RowData row = sources.get(asName);
        if(row!=null){
            for (Map.Entry<String,byte[]> column:
                 data) {
                row.setData(column.getKey(), column.getValue());
            }
        }else{
            sources.put(asName,data);
        }
    }

    public RowData getContent(String name){
        RowData rd=sources.get(name);
        if(rd == null)rd=new RowData();
        return rd;
    }

    public Iterable<String> getSources(){
        return sources.keySet();
    }

    @Override
    public Iterator<Map.Entry<String, RowData>> iterator() {
        return sources.entrySet().iterator();
    }
}
