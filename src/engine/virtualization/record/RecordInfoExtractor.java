package engine.virtualization.record;

import engine.file.streams.ReadByteStream;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public interface RecordInfoExtractor {

    public BigInteger getPrimaryKey(ByteBuffer rbs);
    public BigInteger getPrimaryKey(ReadByteStream rbs);

    public boolean isActiveRecord(ByteBuffer rbs);
    public boolean isActiveRecord(ReadByteStream rbs);

    public void setActiveRecord(Record r,boolean active);
}
