package lib.booleanexpression.entities.elements;

public class Variable extends Element{

    private final String name;

    public Variable(String name){

    	this.name = name;
    	
    }

    public String getName(){
        return name;
    }

    @Override
    public String toString(){
        return getName();
    }

}
