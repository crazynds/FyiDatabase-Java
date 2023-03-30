package sgbd.query.unaryop;

import sgbd.prototype.ComplexRowData;
import sgbd.query.Operator;
import sgbd.query.Tuple;

import java.util.List;
import java.util.Map;

public class FilterColumnsOperator extends UnaryOperator{

    private String content;
    private List<String> columns;

    public FilterColumnsOperator(Operator op, String content, List<String> columns) {
        super(op);
        this.columns = columns;
        this.content = content;
    }

    @Override
    public void open() {
        operator.open();
    }

    @Override
    public Tuple next() {
        Tuple t = operator.next().clone();
        ComplexRowData row = t.getContent(content);
        if(row!=null){
            for(String colName: columns)
                row.unset(colName);
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
        if(map.containsKey(content)){
            for(String colName: columns)
                map.get(content).remove(colName);
        }
        return map;
    }
}
