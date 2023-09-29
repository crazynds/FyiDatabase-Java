package engine.virtualization.record;

import engine.file.streams.ReadByteStream;
import lib.BigKey;


import java.nio.ByteBuffer;

public interface RecordInfoExtractor {

    public BigKey getPrimaryKey(ByteBuffer rbs);
    public BigKey getPrimaryKey(ReadByteStream rbs);

    public boolean isActiveRecord(ByteBuffer rbs);
    public boolean isActiveRecord(ReadByteStream rbs);

    public void setActiveRecord(Record r,boolean active);
}
