package sgbd.prototype.query.fields;

import engine.exceptions.DataBaseException;
import sgbd.prototype.BData;
import sgbd.prototype.column.Metadata;
import sgbd.util.global.Util;

public class IntField extends Field {

    public IntField(Metadata col, BData data) {
        super(col, data);
        if(Util.typeOfColumn(col)!="int")throw new DataBaseException("IntField->Constructor","IntField needs to be int");
    }


    @Override
    public int compareTo(Field f) {
        if(!f.isInt())return 0;
        return data.getInt().compareTo(f.data.getInt());
    }
}
