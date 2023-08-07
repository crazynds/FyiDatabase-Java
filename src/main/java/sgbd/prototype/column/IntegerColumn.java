package sgbd.prototype.column;

import sgbd.prototype.metadata.Metadata;

public class IntegerColumn extends Column{
    public IntegerColumn(String name,boolean primaryKey) {
        super(name, (short) 4, primaryKey ? Metadata.PRIMARY_KEY:Metadata.NONE);
    }
    public IntegerColumn(String name) {
        this(name,false);
    }
}
