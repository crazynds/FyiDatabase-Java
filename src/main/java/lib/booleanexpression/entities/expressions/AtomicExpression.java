package lib.booleanexpression.entities.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import lib.booleanexpression.entities.elements.Element;
import lib.booleanexpression.entities.elements.Value;
import lib.booleanexpression.entities.elements.Variable;
import lib.booleanexpression.enums.RelationalOperator;
import lib.booleanexpression.enums.Result;

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

    public boolean hasColumn(){
        return isFirstElementAColumn() || isSecondElementAColumn();
    }

    public List<Variable> getAllColumns(){

        if(!hasColumn()) return List.of();

        List<Variable> allVariables = new ArrayList<>();

        if(isFirstElementAColumn())
            allVariables.add((Variable) firstElement);

        if(isSecondElementAColumn())
            allVariables.add((Variable) secondElement);

        return allVariables;

    }

    public List<Variable> getMandatoryVariables() {

        if(!hasColumn()) return List.of();

        List<Variable> mandatoryVariables = new ArrayList<>();

        if(isFirstElementAColumn())
            mandatoryVariables.add((Variable) firstElement);

        if(isSecondElementAColumn())
            mandatoryVariables.add((Variable) secondElement);

        return mandatoryVariables;

    }

    public Result solve(){
        AtomicExpression expression = this;
        if(expression.hasColumn()){
            if(expression.isFirstElementAColumn() && !columnHasValue((Variable) expression.getFirstElement()))
                return Result.NOT_READY;
            if(expression.isSecondElementAColumn() && !columnHasValue((Variable) expression.getSecondElement()))
                return Result.NOT_READY;
        }

        Object obj1 = expression.isFirstElementAColumn() ? columns.get(expression.getFirstElement()) :
                ((Value)expression.getFirstElement()).getValue();

        Object obj2 = expression.isSecondElementAColumn() ? columns.get(expression.getSecondElement()) :
                ((Value)expression.getSecondElement()).getValue();

        if(!Objects.equals(obj1.getClass(), obj2.getClass())) return Result.FALSE;


        if (obj1 instanceof Comparable) {
            int compareResult = ((Comparable) obj1).compareTo(obj2);

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
        } else {
            throw new UnsupportedOperationException("Objects are not comparable");
        }

    }

    public Element getFirstElement() {
        return firstElement;
    }

    public Element getSecondElement() {
        return secondElement;
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
