package lib.booleanexpression.entities.elements;

import java.util.Objects;

public class Value extends Element{

    private final Object value;

    public Value(Object value){

        this.value = value;

    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return Objects.toString(value);
    }
}
