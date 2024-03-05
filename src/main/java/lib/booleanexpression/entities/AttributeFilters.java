package lib.booleanexpression.entities;


import lib.booleanexpression.entities.elements.Value;

import java.security.KeyPair;
import java.util.HashMap;
import java.util.Map;

public class AttributeFilters {


    private Map<String, Map.Entry<Value, Value>> map = new HashMap<>();

    public AttributeFilters(){

    }

    public void merge(AttributeFilters other){
        for (Map.Entry<String, Map.Entry<Value, Value>> entry:
             other.map.entrySet()) {
            addEntry(entry.getKey(),entry.getValue().getKey(),entry.getValue().getValue());
        }
    }

    public void addEntry(String column, Value min, Value max){
        if(map.get(column)==null){
            map.put(column,Map.entry(min,max));
        }else {
            Map.Entry<Value,Value> entry = map.get(column);
            Value a = entry.getKey();
            Value b = entry.getValue();
            a = (a!=null && (min==null || a.getField().compareTo(min.getField())<=0))? a : min;
            b = (b!=null && (max==null || b.getField().compareTo(max.getField())<=0))? b : max;
            map.put(column,Map.entry(a,b));
        }
    }

    public Map.Entry<Value,Value> getColumnFilter(String column){
        return map.get(column);
    }
}