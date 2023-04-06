package engine.file.blocks.commitable;

import java.util.LinkedList;

public interface WriteBack {

	public void commitWrites(LinkedList<WriteCache> list);
}
