package lib.booleanexpression.entities;

import sgbd.prototype.query.fields.Field;

import java.util.HashMap;
import java.util.Map;

public class AttributeFilters {


    private Map<String, Map.Entry<Field, Field>> map = new HashMap<>();

    public AttributeFilters(){

    }

    public void merge(AttributeFilters other){
        for (Map.Entry<String, Map.Entry<Field, Field>> entry:
             other.map.entrySet()) {
            addEntry(entry.getKey(),entry.getValue().getKey(),entry.getValue().getValue());
        }
    }

    public void addEntry(String column, Field min, Field max){
        if(map.get(column)==null){
            map.put(column, new Map.Entry<Field, Field>() {
                @Override
                public Field getKey() {
                    return min;
                }

                @Override
                public Field getValue() {
                    return max;
                }
                @Override
                public Field setValue(Field Field) {
                    return null;
                }
            });
        }else {
            Map.Entry<Field,Field> entry = map.get(column);
            Field a = entry.getKey();
            Field b = entry.getValue();
            a = (a!=null && (min==null || a.compareTo(min)<=0))? a : min;
            b = (b!=null && (max==null || b.compareTo(max)<=0))? b : max;
            Field finalA = a;
            Field finalB = b;
            map.put(column, new Map.Entry<Field, Field>() {
                @Override
                public Field getKey() {
                    return finalA;
                }

                @Override
                public Field getValue() {
                    return finalB;
                }

                @Override
                public Field setValue(Field value) {
                    return null;
                }
            });
        }
    }

    public Map.Entry<Field,Field> getColumnFilter(String column){
        return map.get(column);
    }
}
