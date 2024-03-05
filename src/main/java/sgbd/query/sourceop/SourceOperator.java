package sgbd.query.sourceop;

import lib.booleanexpression.entities.expressions.BooleanExpression;
import sgbd.query.AttributeFilters;
import sgbd.query.Operator;
import sgbd.source.Source;

import java.util.List;

public abstract class SourceOperator implements Operator {

    protected Source source;
    protected String asName;
    public SourceOperator(Source source){
        this.source = source;
        this.asName= source.getSourceName();
    }

    @Override
    public void lookup(AttributeFilters filters) {
        // apply filters
    }

    public void asName(String name){
        asName=name;
    }
    public String sourceName(){
        return asName;
    }

    public List<Source> getSources(){
        return List.of(source);
    }

    @Override
    public void freeResources(){

    }

}
