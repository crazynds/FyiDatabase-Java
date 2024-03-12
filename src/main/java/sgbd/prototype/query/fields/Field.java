package sgbd.prototype.query.fields;

import sgbd.prototype.BData;
import sgbd.prototype.metadata.Metadata;
import sgbd.util.global.Util;

public abstract class Field<T> implements Comparable<Field>{

    protected static final int NOT_DEFINED = -1;
    protected static final int NULL_COMPARE = -1;

    protected final BData data;
    protected final Metadata metadata;
    protected T bufferedData;
    protected Field(Metadata metadata,BData data) {
        this.data = data;
        this.metadata = metadata;
        //this.bufferedData = constructData();
    }

    protected Field(Metadata metadata,BData data,T value){
        this.bufferedData = value;
        this.metadata = metadata;
        this.data = data;
    }

    public static Field createField(Metadata metadata,BData data){
        switch (Util.typeOfColumn(metadata)){
            case "boolean":
                return new BooleanField(metadata,data);
            case "string":
                return new StringField(metadata,data);
            case "long":
                return new LongField(metadata,data);
            case "int":
                return new IntegerField(metadata,data);
            case "double":
                return new DoubleField(metadata,data);
            case "float":
                return new FloatField(metadata,data);
            case "null":
            default:
                return new NullField(metadata);
        }
    }

    protected abstract T constructData();

    public T getBufferedData(){
        if(bufferedData == null)
            bufferedData = constructData();
        return bufferedData;
    }

    public BData getBData(){
        return data;
    }
    public Metadata getMetadata(){
        return metadata;
    }

    public Integer getInt(){
        return (Integer)getBufferedData();
    }
    public Long getLong(){
        return (Long)getBufferedData();
    }
    public Boolean getBoolean(){
        return (Boolean)getBufferedData();
    }
    public Float getFloat(){
        return (Float)getBufferedData();
    }
    public Double getDouble(){
        return (Double)getBufferedData();
    }
    public String getString(){
        return (String)getBufferedData();
    }

    public abstract int compareTo(Field f);

    public long bufferByteSize(){
        if(data!=null){
            return data.length();
        }
        return 0;
    }

}
