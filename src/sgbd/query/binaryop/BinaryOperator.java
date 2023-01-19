package sgbd.query.binaryop;

import sgbd.query.Operator;
import sgbd.table.Table;

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


    public List<Table> getSources(){
        List<Table> rTable = right.getSources();
        List<Table> lTable = left.getSources();
        return Stream.concat(rTable.stream(),lTable.stream()).collect(Collectors.toList());
    }

    @Override
    public Map<String, List<String>> getContentInfo() {
        Map<String, List<String>> rTable = right.getContentInfo();
        Map<String, List<String>> lTable = left.getContentInfo();
        for(Map.Entry<String,List<String>> a: rTable.entrySet()){
            if(lTable.containsKey(a.getKey())){
                a.setValue(Stream.concat(a.getValue().stream(),lTable.get(a.getKey()).stream()).collect(Collectors.toList()));
                lTable.remove(a.getKey());
            }
        }
        return Stream.concat(rTable.entrySet().stream(), lTable.entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
