package lib.booleanexpression.entities.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import lib.booleanexpression.entities.AttributeFilters;
import lib.booleanexpression.entities.elements.Element;
import lib.booleanexpression.entities.elements.Value;
import lib.booleanexpression.entities.elements.Variable;
import lib.booleanexpression.enums.RelationalOperator;
import lib.booleanexpression.enums.Result;
import sgbd.prototype.RowData;
import sgbd.prototype.query.Tuple;
import sgbd.prototype.query.fields.Field;

public class AtomicExpression extends BooleanExpression{

    private final Element firstElement;
    private final Element secondElement;
    private final RelationalOperator relationalOperator;

    public AtomicExpression(Element firstElement, Element secondElement, RelationalOperator relationalOperator){

        this(firstElement, secondElement, relationalOperator, true);

    }

    public AtomicExpression(Element firstElement, Element secondElement, RelationalOperator relationalOperator, boolean booleanValue){

        super(booleanValue);

        this.firstElement = firstElement;
        this.secondElement = secondElement;

        this.relationalOperator = relationalOperator;

    }

    public boolean isFirstElementAColumn(){
        return firstElement instanceof Variable;
    }

    public boolean isSecondElementAColumn(){
        return secondElement instanceof Variable;
    }

    public Result solve(){
        AtomicExpression expression = this;
        Field obj1 = firstElement.getField(),obj2 = secondElement.getField();

        if(obj1==null || obj2==null)return Result.NOT_READY;

        int compareResult = obj1.compareTo(obj2);

        return switch (expression.getRelationalOperator()) {
            case LESS_THAN -> Result.evaluate(compareResult < 0);
            case GREATER_THAN -> Result.evaluate(compareResult > 0);
            case GREATER_THAN_OR_EQUAL -> Result.evaluate(compareResult >= 0);
            case LESS_THAN_OR_EQUAL -> Result.evaluate(compareResult <= 0);
            case EQUAL -> Result.evaluate(compareResult == 0);
            case NOT_EQUAL -> Result.evaluate(compareResult != 0);
            case IS -> Result.evaluate(true);
            case IS_NOT -> Result.evaluate(false);
        };
    }

    @Override
    public void clear() {
        if(firstElement instanceof Variable firstVar)firstVar.setField(null);
        if(secondElement instanceof Variable secondVar)secondVar.setField(null);
    }

    @Override
    public void applyTuple(Tuple t) {
        if(firstElement instanceof Variable firstVar){
            Field f = t.getField(firstVar.getNames());
            if(f!=null)
                firstVar.setField(f);
        }
        if(secondElement instanceof Variable secondVar){
            Field f = t.getField(secondVar.getNames());
            if(f!=null)
                secondVar.setField(f);
        }
    }

    @Override
    public void applyAttributeFilters(AttributeFilters filter) {
        Variable var = null;
        Field field = null;
        if(firstElement instanceof Variable firstVar && firstVar.getField()==null){
            field = secondElement.getField();
            var = firstVar;
        }else if(secondElement instanceof Variable secondVar && secondVar.getField()==null){
            field = firstElement.getField();
            var = secondVar;
        }
        if(var==null || field==null)return;
        switch (getRelationalOperator()){
            case LESS_THAN, LESS_THAN_OR_EQUAL:
                filter.addEntry(var.toString(),null,field);
                break;
            case GREATER_THAN, GREATER_THAN_OR_EQUAL:
                filter.addEntry(var.toString(),field,null);
                break;
            case EQUAL:
                filter.addEntry(var.toString(),field,field);
                break;
            case NOT_EQUAL:
            case IS:
            case IS_NOT:
            default:
                return;
        }
    }


    public RelationalOperator getRelationalOperator() {
        return relationalOperator;
    }
    
    public Element getFirstElement() {
    	return firstElement;
    }

    public Element getSecondElement() {
    	return secondElement;
    }
    
    @Override
    public String toString(){

        String txt = firstElement + " " + relationalOperator + " " + secondElement;

        if(isFalse()) txt = "!(" + txt + ")";

        return txt;

    }


}
