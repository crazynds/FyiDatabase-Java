package lib.booleanexpression.entities.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
            String[] names = firstVar.getName().split(" ");
            obj1 = t.getField(names);
        }else{
            // Como saber o tipo do field estático???
        }
        if(secondElement instanceof Variable secondVar){
            String[] names = secondVar.getName().split(" ");
            obj2 = t.getField(names);

        }else{
            // Como saber o tipo do field estático???
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

    public RelationalOperator getRelationalOperator() {
        return relationalOperator;
    }

    @Override
    public String toString(){

        String txt = firstElement + " " + relationalOperator + " " + secondElement;

        if(isFalse()) txt = "!(" + txt + ")";

        return txt;

    }


}
