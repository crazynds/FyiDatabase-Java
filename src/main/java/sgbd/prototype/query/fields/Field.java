package sgbd.prototype.query.fields;

import sgbd.prototype.BData;
import sgbd.prototype.column.Metadata;

public abstract class Field implements Comparable<Field>{

    protected final BData data;
    protected final Metadata col;
    public Field(Metadata col,BData data) {
        this.data = data;
        this.col = col;
    }


    public boolean isInt(){
        return false;
    }
    public boolean isLong(){
        return false;
    }
    public boolean isBoolean(){
        return false;
    }
    public boolean isFloat(){
        return false;
    }
    public boolean isDouble(){
        return false;
    }
    public boolean isString() {
        return false;
    }

    public Integer getInt(){
        return data.getInt();
    }
    public Long getLong(){
        return data.getLong();
    }
    public Boolean getBoolean(){
        return data.getBoolean();
    }
    public Float getFloat(){
        return data.getFloat();
    }
    public Double getDouble(){
        return data.getDouble();
    }
    public String getString(){
        return data.getString();
    }

    public abstract int compareTo(Field f);

}
