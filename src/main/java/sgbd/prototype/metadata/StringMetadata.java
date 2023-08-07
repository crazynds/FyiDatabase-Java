package sgbd.prototype.metadata;

public class StringMetadata extends Metadata{
    public StringMetadata(short size) {
        super(size, (short) (Metadata.STRING|Metadata.DINAMIC_COLUMN_SIZE));
    }
}
