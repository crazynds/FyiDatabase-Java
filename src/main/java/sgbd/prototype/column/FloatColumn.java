package sgbd.prototype.column;

import sgbd.prototype.metadata.Metadata;

public class FloatColumn extends Column{
    public FloatColumn(String name) {
        super(name, (short) 4, Metadata.FLOATING_POINT);
    }
}
