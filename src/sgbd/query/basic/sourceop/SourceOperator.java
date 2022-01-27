package sgbd.query.basic.sourceop;

import sgbd.query.basic.Operator;
import sgbd.table.Table;

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

}
