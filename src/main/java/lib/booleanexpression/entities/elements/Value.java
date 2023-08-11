package lib.booleanexpression.entities.elements;

import sgbd.prototype.query.fields.Field;

import java.util.Objects;

public class Value extends Element{

    private final Field value;

    public Value(Field f){

        this.value = f;

    }

    public Field getField() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
