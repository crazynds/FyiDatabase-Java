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
    
    public static RelationalOperator getOperator(String symbol) {
    	
    	return switch(symbol) {
    		
    	case "<" -> LESS_THAN;
    	case ">" -> GREATER_THAN;
    	case ">=", "≥" -> GREATER_THAN_OR_EQUAL;
    	case "<=", "≤" -> LESS_THAN_OR_EQUAL;
    	case "=", "==" -> EQUAL;
    	case "!=", "≠" -> NOT_EQUAL;
    	case "is", "IS" -> IS;
    	case "is not", "IS NOT" -> IS_NOT;
    	
    	default -> throw new IllegalArgumentException("this is not a valid operator");
    	
    	};
    	
    }
    
    public final String[] symbols;
    
}
