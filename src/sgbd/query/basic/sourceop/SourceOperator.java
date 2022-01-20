package sgbd.query.basic.sourceop;

import sgbd.query.basic.Operator;

public abstract  class SourceOperator implements Operator {

    public abstract void asName(String name);
    public abstract String sourceName();

}
