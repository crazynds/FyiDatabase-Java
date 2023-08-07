package sgbd.prototype.column;

import sgbd.prototype.metadata.Metadata;

public class LongColumn extends Column{
    public LongColumn(String name, boolean primaryKey) {
        super(name, (short) 8, primaryKey? Metadata.PRIMARY_KEY:Metadata.NONE);
    }
    public LongColumn(String name){
        this(name,false);
    }
}
