package sgbd.prototype.column;

public class DoubleColumn extends Column{
    public DoubleColumn(String name) {
        super(name, (short) 8, Metadata.FLOATING_POINT);
    }
}
