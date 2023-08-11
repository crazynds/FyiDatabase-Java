package sgbd.prototype.column;

import sgbd.prototype.metadata.Metadata;

public class IntegerColumn extends Column{
    public IntegerColumn(String name,boolean primaryKey,boolean nullable) {
        super(name, (short) 4, (primaryKey ? Metadata.PRIMARY_KEY:(nullable?Metadata.CAN_NULL_COLUMN:Metadata.NONE)));
    }
    public IntegerColumn(String name,boolean primaryKey) {
        this(name,primaryKey,false);
    }
    public IntegerColumn(String name) {
        this(name,false);
    }
}
