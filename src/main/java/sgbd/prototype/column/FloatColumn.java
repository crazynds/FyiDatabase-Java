package sgbd.prototype.column;

public class FloatColumn extends Column{
    public FloatColumn(String name) {
        super(name, (short) 4, Metadata.FLOATING_POINT);
    }
}
