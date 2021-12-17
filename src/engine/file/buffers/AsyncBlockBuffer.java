package engine.file.buffers;

import engine.file.Block;
import engine.file.streams.BlockStream;
import engine.file.streams.ReadByteStream;
import engine.file.streams.WriteByteStream;

public abstract class AsyncBlockBuffer extends BlockBuffer {

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        this.flush();
    }

    @Override
    public Block getBlockIfExistInBuffer(int num) {
        return null;
    }

    @Override
    public void hintBlock(int num) {

    }

    @Override
    public void forceBlock(int num) {

    }

    @Override
    public void clearBuffer() {

    }

    @Override
    public int getBlockSize() {
        return 0;
    }

    @Override
    public int lastBlock() {
        return 0;
    }

    @Override
    public Block readBlock(int pos) {
        return null;
    }

    @Override
    public void readBlock(int pos, byte[] buffer) {

    }

    @Override
    public ReadByteStream getBlockReadByteStream(int block) {
        return null;
    }

    @Override
    public void writeBlock(int pos, Block b) {

    }

    @Override
    public WriteByteStream getBlockWriteByteStream(int block) {
        return null;
    }
}
