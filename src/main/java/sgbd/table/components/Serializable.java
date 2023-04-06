package sgbd.table.components;

import engine.file.streams.ByteStream;

public interface Serializable {

	public void write(ByteStream vb);
	public void read(ByteStream vb);
}
