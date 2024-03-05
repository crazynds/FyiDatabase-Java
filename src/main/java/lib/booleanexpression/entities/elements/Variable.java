package lib.booleanexpression.entities.elements;

import java.util.Arrays;

public class Variable extends Element{

    private final String[] name;
    private final String fullName;

    public Variable(String name){
        this.fullName = name;
    	this.name = name.split("\\.");
    	
    }

    public String[] getNames(){
        return name;
    }

    @Override
    public String toString(){
        return fullName;
    }

}
