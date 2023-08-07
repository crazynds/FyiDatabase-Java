package lib.booleanexpression.entities.expressions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import lib.booleanexpression.enums.LogicalOperator;
import lib.booleanexpression.enums.Result;

public class LogicalExpression extends BooleanExpression implements Iterable<BooleanExpression> {

    private final List<BooleanExpression> expressions = new ArrayList<>();
    private final LogicalOperator logicalOperator;

    public LogicalExpression(LogicalOperator logicalOperator, boolean booleanValue, List<BooleanExpression> list) {
        super(booleanValue);
        this.logicalOperator = logicalOperator;

        for (BooleanExpression expression :
                list) {
            if (expression instanceof LogicalExpression logicalExpression) {
                if (logicalExpression.logicalOperator == logicalOperator) {
                    this.expressions.addAll(logicalExpression.expressions);
                    continue;
                }
            }

            this.expressions.add(expression);
        }
    }

    public LogicalExpression(LogicalOperator logicalOperator, List<BooleanExpression> list) {
        this(logicalOperator, true, list);
    }

    public LogicalExpression(LogicalOperator logicalOperator, BooleanExpression... expressions) {
        this(logicalOperator, true, List.of(expressions));
    }

    public LogicalExpression(LogicalOperator logicalOperator, boolean booleanValue, BooleanExpression... expressions) {
        this(logicalOperator, booleanValue, List.of(expressions));
    }

    public Result solve(){
        LogicalOperator operator = this.getLogicalOperator();
        Result anticipatedResult = Objects.equals(operator, LogicalOperator.OR) ? Result.TRUE : Result.FALSE;

        boolean hasNotReadyVariables = false;

        for(BooleanExpression expression : this.getExpressions()) {

            Result atomicResult = expression.solve();

            if(Objects.equals(atomicResult, anticipatedResult)) return anticipatedResult;
            if(Objects.equals(atomicResult, Result.NOT_READY)) hasNotReadyVariables = true;
        }


        return hasNotReadyVariables ? Result.NOT_READY : (anticipatedResult.val() ? Result.TRUE : Result.FALSE);

    }

    public LogicalOperator getLogicalOperator() {
        return logicalOperator;
    }

    public boolean isAnd() {
        return LogicalOperator.AND.equals(logicalOperator);
    }

    public boolean isOr() {
        return LogicalOperator.OR.equals(logicalOperator);
    }

    public boolean hasAtomicExp() {
        return expressions.stream().anyMatch(x -> x instanceof AtomicExpression);
    }

    public boolean hasColumn(){
        for(BooleanExpression exp : expressions){

            if(exp instanceof AtomicExpression atomic && atomic.hasColumn()) return true;

            if(exp instanceof LogicalExpression log && log.hasColumn()) return true;

        }

        return false;
    }

    public List<BooleanExpression> getExpressions(){
        return Collections.unmodifiableList(expressions);
    }

    @Override
    public Iterator<BooleanExpression> iterator() {
        return expressions.iterator();
    }

    @Override
    public String toString() {

        StringBuilder txt = new StringBuilder();

        if(isFalse()) txt.append("!");
        txt.append("(");

        Iterator<BooleanExpression> iterator = expressions.iterator();
        while(iterator.hasNext()){

            txt.append(iterator.next());

            if(iterator.hasNext()) txt.append(" ").append(logicalOperator).append(" ");

        }

        txt.append(")");

        return txt.toString();

    }

}
