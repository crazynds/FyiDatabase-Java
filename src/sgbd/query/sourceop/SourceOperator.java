package sgbd.query.sourceop;

import sgbd.query.Operator;
import sgbd.table.Table;

import java.util.List;

public abstract class SourceOperator implements Operator {

    protected Table table;
    protected String asName;
    public SourceOperator(Table table){
        this.table = table;
        this.asName= table.getTableName();
    }


    public void asName(String name){
        asName=name;
    }
    public String sourceName(){
        return asName;
    }

    public List<Table> getSources(){
        return List.of(table);
    }

}
