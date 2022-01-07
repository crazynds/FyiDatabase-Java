package engine.file.streams;

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
    public byte[] read(long pos, int len) {
        return rbs.read(pos+offset,len);
    }

    @Override
    public byte[] readSeq(int len) {
        return rbs.read(pointer+offset,len);
    }

    @Override
    public int read(long pos, int len, byte[] buffer, int offset) {
        return rbs.read(pos+this.offset,len,buffer,offset);
    }

    @Override
    public int readSeq(int len, byte[] buffer, int offset) {
        return rbs.read(pointer+this.offset,len,buffer,offset);
    }
}
