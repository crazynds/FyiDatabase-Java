package lib.booleanexpression.entities.expressions;

import lib.booleanexpression.entities.elements.Variable;
import enums.Result;

import java.util.*;

public abstract class BooleanExpression {

    private final boolean booleanValue;

    protected final Map<Variable, Object> columns = new HashMap<>();

    public BooleanExpression(boolean booleanValue){
        this.booleanValue = booleanValue;
    }

    public List<Variable> getAllColumns(){

        List<Variable> allVariables = new ArrayList<>();

        if(this instanceof AtomicExpression atomic && atomic.hasColumn())
            allVariables.addAll(atomic.getAllColumns());

        if(this instanceof LogicalExpression logical){

            List<BooleanExpression> levelExpression = new ArrayList<>(List.of(logical));

            while (!levelExpression.isEmpty()){

                List<BooleanExpression> newLevelExpression = new ArrayList<>();
                for(BooleanExpression exp : levelExpression){

                    if(exp instanceof AtomicExpression atomic && atomic.hasColumn())
                        allVariables.addAll(atomic.getAllColumns());

                    if(exp instanceof LogicalExpression subLogical)
                        newLevelExpression.addAll(subLogical.getExpressions());

                }

                levelExpression = newLevelExpression;

            }

        }

        return allVariables;

    }

    public List<Variable> getMandatoryVariables(){

        List<Variable> mandatoryVariables = new ArrayList<>();

        if(this instanceof AtomicExpression atomic)
            mandatoryVariables.addAll(atomic.getMandatoryVariables());
        else
            mandatoryVariables.addAll(((LogicalExpression)this).getMandatoryVariables());

        return mandatoryVariables;

    }

    public abstract Result solve();


    public void insertColumnValue(Variable variable, Object value){

        getAllColumns().stream().filter(x -> x.equals(variable)).findAny().orElseThrow(NoSuchElementException::new);
        columns.put(variable, value);

    }

    public boolean columnHasValue(Variable variable){

        return columns.get(variable) != null;

    }

    public boolean isFalse(){
        return !booleanValue;
    }

    public abstract boolean hasColumn();

}
