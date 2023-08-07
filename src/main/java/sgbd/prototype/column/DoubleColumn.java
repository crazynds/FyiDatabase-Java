package sgbd.prototype.column;

import sgbd.prototype.metadata.Metadata;

public class DoubleColumn extends Column{
    public DoubleColumn(String name) {
        super(name, (short) 8, Metadata.FLOATING_POINT);
    }
}
