package sgbd.prototype.query.fields;

import engine.exceptions.DataBaseException;
import sgbd.prototype.BData;
import sgbd.prototype.metadata.IntegerMetadata;
import sgbd.prototype.metadata.Metadata;
import sgbd.util.global.Util;
import sgbd.util.global.UtilConversor;

public class IntegerField extends Field<Integer> {

    public IntegerField(Metadata col, BData data) {
        super(col, data);
        if(Util.typeOfColumn(col)!="int")throw new DataBaseException("IntField->Constructor","IntField needs to be int");
    }
    public IntegerField(int value) {
        super(IntegerMetadata.generic, new BData(UtilConversor.intToByteArray(value)), value);
    }

    @Override
    protected Integer constructData() {
        return data.getInt();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null || f.getBufferedData() == null)return NULL_COMPARE;
        if(f.metadata.isString() || f.metadata.isFloat())return f.compareTo(this);
        if(!f.metadata.isInt())return NOT_DEFINED;
        if(f.metadata.getSize()==8)return f.compareTo(this);
        return getBufferedData().compareTo(f.getInt());
    }

}
