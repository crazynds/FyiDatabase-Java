package engine.file.buffers;

public class LIFOBlockBuffer extends FIFOBlockBuffer {
	
	public LIFOBlockBuffer(int bufferSized) {
		super(bufferSized);
	}
	
	@Override
	protected synchronized void addItem(EntryBlock e) {
		blocks.addFirst(e);
	}
}
