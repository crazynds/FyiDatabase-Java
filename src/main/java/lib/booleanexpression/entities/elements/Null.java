package lib.booleanexpression.entities.elements;

import sgbd.prototype.query.fields.Field;
import sgbd.prototype.query.fields.NullField;

public class Null extends Element{

    @Override
    public Field getField() {
        return NullField.generic;
    }
}
