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

    public Result solve(Tuple t){
        AtomicExpression expression = this;
        Field obj1 = null,obj2 = null;
        if(firstElement instanceof Variable firstVar){
            obj1 = t.getField(firstVar.getNames());
        }else{
            obj1 = ((Value)firstElement).getField();
        }
        if(secondElement instanceof Variable secondVar){
            obj2 = t.getField(secondVar.getNames());

        }else{
            obj2 = ((Value)secondElement).getField();
        }

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
    public void applyAttributeFilters(AttributeFilters filter) {
        Variable var = null;
        Value val = null;
        if(firstElement instanceof Value && secondElement instanceof Variable){
            var = (Variable) secondElement;
            val = (Value) firstElement;
        }else if(secondElement instanceof Value && firstElement instanceof Variable){
            var = (Variable) firstElement;
            val = (Value) secondElement;
        }
        if(var==null || val==null)return;
        switch (getRelationalOperator()){
            case LESS_THAN, LESS_THAN_OR_EQUAL:
                filter.addEntry(var.toString(),null,val);
                break;
            case GREATER_THAN, GREATER_THAN_OR_EQUAL:
                filter.addEntry(var.toString(),val,null);
                break;
            case EQUAL:
                filter.addEntry(var.toString(),val,val);
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
