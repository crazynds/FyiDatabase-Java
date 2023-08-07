package lib.booleanexpression.enums;

public enum RelationalOperator {
    LESS_THAN("<"),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">=", "≥"),
    LESS_THAN_OR_EQUAL("<=", "≤"),
    EQUAL("==", "="),
    NOT_EQUAL("!=", "≠"),
    IS("is", "IS"),
    IS_NOT("is not", "IS NOT");
    
    RelationalOperator(String ...symbol){
    	
    	this.symbols = symbol;
    	
    }
    
    public final String[] symbols;
    
}
