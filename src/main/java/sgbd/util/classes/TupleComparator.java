/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sgbd.util.classes;

import sgbd.prototype.column.Column;
import sgbd.prototype.query.Tuple;
import sgbd.util.global.Util;

import java.util.Arrays;
import java.util.Comparator;

/**
 *
 * @author Sergio
 */
public class TupleComparator implements Comparator<Tuple>{
    private ResourceName resource;

    public TupleComparator(){
        this(null);
    }

    public TupleComparator(ResourceName resource){
        this.resource = resource;
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        if(resource==null)return t1.compareTo(t2);
        Column column = t1.getContent(resource.getSource()).getMeta(resource.getColumn());
        switch (Util.typeOfColumn(column)){
            case "int":
                Integer i1 = t1.getContent(resource.getSource()).getInt(resource.getColumn());
                Integer i2 = t2.getContent(resource.getSource()).getInt(resource.getColumn());
                return Integer.compare(i1,i2);
            case "float":
                Float f1 = t1.getContent(resource.getSource()).getFloat(resource.getColumn());
                Float f2 = t2.getContent(resource.getSource()).getFloat(resource.getColumn());
                return Float.compare(f1,f2);
            case "double":
                Double d1 = t1.getContent(resource.getSource()).getDouble(resource.getColumn());
                Double d2 = t2.getContent(resource.getSource()).getDouble(resource.getColumn());
                return Double.compare(d1,d2);
            case "bool":
                Boolean b1 = t1.getContent(resource.getSource()).getBoolean(resource.getColumn());
                Boolean b2 = t2.getContent(resource.getSource()).getBoolean(resource.getColumn());
                return Boolean.compare(b1,b2);
            case "string":
                String s1 = t1.getContent(resource.getSource()).getString(resource.getColumn());
                String s2 = t2.getContent(resource.getSource()).getString(resource.getColumn());
                if(s1==null)return -1;
                return s1.compareTo(s2);
            default:
                byte[] a1 = t1.getContent(resource.getSource()).getData(resource.getColumn());
                byte[] a2 = t2.getContent(resource.getSource()).getData(resource.getColumn());
                return Arrays.compare(a1,a2);
        }
    }
    
}
