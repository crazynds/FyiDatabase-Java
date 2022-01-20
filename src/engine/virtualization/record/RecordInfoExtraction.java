package engine.virtualization.record;

import engine.file.streams.ReadByteStream;

import java.math.BigInteger;

public interface RecordInfoExtraction {

    public BigInteger getPrimaryKey(Record r);
    public BigInteger getPrimaryKey(ReadByteStream rbs);

    public boolean isActiveRecord(Record r);
    public boolean isActiveRecord(ReadByteStream rbs);

    public void setActiveRecord(Record r,boolean active);
}
