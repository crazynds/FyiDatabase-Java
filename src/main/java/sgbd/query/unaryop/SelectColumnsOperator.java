package sgbd.query.unaryop;

import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.prototype.query.Tuple;

import java.util.*;

public class SelectColumnsOperator extends UnaryOperator{
    private HashMap<String,String[]> srcColumns;

    public SelectColumnsOperator(Operator op, List<String> srcColumns) {
        super(op);
        this.srcColumns = new HashMap<>();
        for (String s:
                srcColumns) {
            String[] vals = s.split("\\.");
            String[] v = {vals[0],vals[1]};
            this.srcColumns.put(s,v);
        }
    }

    @Override
    public Tuple next() {
        Tuple t = operator.next();
        Tuple newT = new Tuple();

        for(Map.Entry<String,String[]> srcColumn:srcColumns.entrySet()){
            String src = srcColumn.getValue()[0];
            String column = srcColumn.getValue()[1];
            ComplexRowData row = t.getContent(src);
            newT.getContent(src).setBData(column,row.getBData(column),row.getMeta(column));
        }
        return newT;
    }

    @Override
    public boolean hasNext() {
        return operator.hasNext();
    }

    @Override
    public Map<String, List<String>> getContentInfo() {
        Map<String, List<String>> map = super.getContentInfo();
        LinkedHashMap<String,List<String>> nMap = new LinkedHashMap<>();
        for(Map.Entry<String,String[]> srcColumn:srcColumns.entrySet()){
            String src = srcColumn.getValue()[0];
            String column = srcColumn.getValue()[1];
            if(map.containsKey(src) && map.get(src).contains(column)){
                if(nMap.containsKey(src)==false)
                    nMap.put(src,new ArrayList<>());
                nMap.get(src).add(column);
            }
        }
        return nMap;
    }
}
