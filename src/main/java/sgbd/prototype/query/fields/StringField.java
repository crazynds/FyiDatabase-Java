package sgbd.prototype.query.fields;

import sgbd.prototype.BData;
import sgbd.prototype.metadata.IntegerMetadata;
import sgbd.prototype.metadata.Metadata;
import sgbd.prototype.metadata.StringMetadata;
import sgbd.util.global.Util;

public class StringField extends Field<String>{
    public StringField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    public StringField(String str) {
        super(new StringMetadata((short)str.length()), str);
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
                val = String.valueOf(f.getBoolean());
                break;
            case "string":
                val = String.valueOf(f.getString());
                break;
            case "long":
                val = String.valueOf(f.getLong());
                break;
            case "int":
                val = String.valueOf(f.getInt());
                break;
            case "double":
                val = String.valueOf(f.getDouble());
                break;
            case "float":
                val = String.valueOf(f.getFloat());
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
