package sgbd.query.unaryop;

import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FilterColumnsOperator extends UnaryOperator{

    private List<String[]> srcColumns;

    public FilterColumnsOperator(Operator op, String content, List<String> columns) {
        super(op);
        ArrayList<String[]> arr = new ArrayList<>();
        for (String s:
             columns) {
            String[] v = {content,s};
            arr.add(v);
        }
        srcColumns = arr;
    }
    public FilterColumnsOperator(Operator op, List<String> srcColumns) {
        super(op);
        ArrayList<String[]> arr = new ArrayList<>();
        for (String s:
                srcColumns) {
            String[] vals = s.split("\\.");
            String[] v = {vals[0],vals[1]};
            arr.add(v);
        }
        this.srcColumns = arr;
    }

    @Override
    public void open() {
        operator.open();
    }

    @Override
    public Tuple next() {
        Tuple t = operator.next().clone();
        for(String[] srcColumn:srcColumns){
            String src = srcColumn[0];
            String column = srcColumn[1];
            ComplexRowData row = t.getContent(src);
            t.getContent(src).unset(column);
        }
        return t;
    }

    @Override
    public boolean hasNext() {
        return operator.hasNext();
    }

    @Override
    public void close() {
        operator.close();
    }

    @Override
    public Map<String, List<String>> getContentInfo() {
        Map<String, List<String>> map = super.getContentInfo();
        for(String[] srcColumn:srcColumns){
            String src = srcColumn[0];
            String column = srcColumn[1];
            if(map.containsKey(src)){
                map.get(src).remove(column);
            }
        }
        return map;
    }
}
