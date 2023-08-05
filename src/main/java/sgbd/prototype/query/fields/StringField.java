package sgbd.prototype.query.fields;

import sgbd.prototype.BData;
import sgbd.prototype.metadata.Metadata;
import sgbd.util.global.Util;

public class StringField extends Field<String>{
    public StringField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    @Override
    protected String constructData() {
        return data.getString();
    }

    @Override
    public int compareTo(Field f) {
        if(f == null)return NULL_COMPARE;
        String val;
        if(!f.metadata.isString()){
            switch (Util.typeOfColumn(f.metadata)){
            case "boolean":
            case "string":
            case "long":
            case "int":
            case "double":
            case "float":
                val = String.valueOf(f.getBoolean());
                break;
            case "null":
            default:
                val = null;
            }
        }else{
            val = f.getString();
        }
        return getBufferedData().compareTo(val);
    }
}
