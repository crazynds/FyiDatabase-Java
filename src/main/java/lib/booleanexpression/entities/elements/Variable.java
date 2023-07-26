package lib.booleanexpression.entities.elements;

public class Variable extends Element{

    private final String columnName;
    private final String columnSource;

    public Variable(String columnSource,String columnName){

        this.columnName = columnName;
        this.columnSource = columnSource;

    }

    public String getColumnName(){
        return columnName;
    }

    public String getColumnSource(){
        return columnSource;
    }

    public String getSourceAndColumn(){
        return columnSource+"."+columnName;
    }

    @Override
    public String toString(){
        return getSourceAndColumn();
    }

}
