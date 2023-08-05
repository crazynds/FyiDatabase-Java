package sgbd.prototype.column;

import sgbd.prototype.metadata.Metadata;

public class BooleanColumn extends Column{
    public BooleanColumn(String name) {
        super(name, (short) 1, Metadata.BOOLEAN);
    }
}
