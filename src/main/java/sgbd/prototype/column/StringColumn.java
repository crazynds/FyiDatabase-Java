package sgbd.prototype.column;

public class StringColumn extends Column{
    public StringColumn(String name, short size) {
        super(name, size, (short) (Metadata.STRING|Metadata.DINAMIC_COLUMN_SIZE));
    }
    public StringColumn(String name){
        this(name, (short) 255);
    }
}
