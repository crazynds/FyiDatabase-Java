package sgbd.prototype.query.fields;

import sgbd.prototype.BData;
import sgbd.prototype.metadata.Metadata;

import java.util.Arrays;

public class BinaryField extends Field<byte[]>{
    public BinaryField(Metadata metadata, BData data) {
        super(metadata, data);
    }

    @Override
    protected byte[] constructData() {
        return data.getData();
    }

    @Override
    public int compareTo(Field f) {
        if(f==null)return 0;
        return Arrays.compare(getBufferedData(),f.data.getData());
    }
}
