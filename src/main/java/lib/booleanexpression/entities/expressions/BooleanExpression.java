package lib.booleanexpression.entities.expressions;

import lib.booleanexpression.entities.elements.Variable;
import lib.booleanexpression.enums.*;
import sgbd.prototype.query.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public abstract class BooleanExpression {

    private final boolean booleanValue;
    public BooleanExpression(boolean booleanValue){
        this.booleanValue = booleanValue;
    }

    public abstract Result solve(Tuple t);

    public boolean isFalse(){
        return !booleanValue;
    }

}
