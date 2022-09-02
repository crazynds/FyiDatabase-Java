package engine.file.streams;

import java.nio.ByteBuffer;

public class ReferenceReadByteStream implements ReadByteStream{
    private long pointer = 0;

    private ReadByteStream rbs;
    private long offset;

    public ReferenceReadByteStream(){
        this.rbs = null;
        this.offset = 0;
    }
    public ReferenceReadByteStream(ReadByteStream rbs, long offset){
        this.rbs = rbs;
        this.offset = offset;
    }

    public void setReference(ReadByteStream rbs, long offset) {
        this.rbs = rbs;
        this.offset = offset;
    }
    public long getOffset() {
        return offset;
    }
    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public void setPointer(long pos) {
        this.pointer = pos;
    }

    @Override
    public long getPointer() {
        return pointer;
    }

    @Override
    public ByteBuffer read(long pos, int len) {
        return rbs.read(pos+offset,len);
    }

    @Override
    public ByteBuffer readSeq(int len) {
        return rbs.read(pointer+offset,len);
    }

    @Override
    public int read(long pos, byte[] buffer, int offset,int len) {
        return rbs.read(pos+this.offset,buffer,offset,len);
    }

    @Override
    public int readSeq(byte[] buffer, int offset,int len) {
        int inc = rbs.read(pointer+this.offset,buffer,len,offset);
        pointer+=inc;
        return inc;
    }

    @Override
    public int read(long pos, ByteBuffer buffer, int offset, int len) {
        return rbs.read(pos+this.offset,buffer,offset,len);
    }

    @Override
    public int readSeq(ByteBuffer buffer, int offset, int len) {
        int inc = rbs.read(pointer+this.offset,buffer,len,offset);
        pointer+=inc;
        return inc;
    }
}
