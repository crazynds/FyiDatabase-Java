package lib.booleanexpression.entities.elements;

import sgbd.prototype.query.fields.Field;

import java.util.Arrays;

public class Variable extends Element{

    private final String[] name;
    private final String fullName;

    private Field field;


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

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }
}
