package engine.virtualization.interfaces;

import engine.virtualization.record.Record;

public interface StorageEventHandler {

    public void updateRecord(Record r, long key);

}
