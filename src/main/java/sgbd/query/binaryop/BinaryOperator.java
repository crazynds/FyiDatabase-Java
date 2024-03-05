package sgbd.query.binaryop;

import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.query.AttributeFilters;
import sgbd.query.Operator;
import sgbd.source.Source;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BinaryOperator implements Operator {

    protected Operator left,right;
    public BinaryOperator(Operator left, Operator right){
        this.left=left;
        this.right=right;
    }

    @Override
    public void lookup(AttributeFilters filters) {
        // ignore filters
    }

    public Operator getLeftOperator(){
        return left;
    }

    public Operator getRightOperator(){
        return right;
    }

    public void setLeftOperator(Operator op){
        this.left = op;
    }
    public void setRightOperator(Operator op){
        this.right = op;
    }

    @Override
    public void open() {
        this.left.open();
        this.right.open();
    }

    @Override
    public void close() {
        this.left.close();
        this.right.close();
    }

    @Override
    public void freeResources() {
        this.left.freeResources();
        this.right.freeResources();
    }

    public List<Source> getSources(){
        List<Source> lTable = left.getSources();
        List<Source> rTable = right.getSources();
        return Stream.concat(lTable.stream(),rTable.stream()).collect(Collectors.toList());
    }

    @Override
    public Map<String, List<String>> getContentInfo() {
        return getStringListMap(left, right);
    }

    protected static Map<String, List<String>> getStringListMap(Operator left, Operator right) {
        Map<String, List<String>> lTable = left.getContentInfo();
        Map<String, List<String>> rTable = right.getContentInfo();
        for(Map.Entry<String,List<String>> a: lTable.entrySet()){
            if(rTable.containsKey(a.getKey())){
                a.setValue(Stream.concat(a.getValue().stream(),rTable.get(a.getKey()).stream()).collect(Collectors.toList()));
                rTable.remove(a.getKey());
            }
        }
        return Stream.concat(lTable.entrySet().stream(), rTable.entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
