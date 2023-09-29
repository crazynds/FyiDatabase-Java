package engine.storage.sorted;

import engine.storage.common.AnonymousStorageRecord;
import engine.virtualization.record.RecordInfoExtractor;
import lib.BigKey;


public abstract class SortedAnonymousStorageRecord extends PkStorageRecord<BigKey> {

    protected RecordInfoExtractor extractor;


    public SortedAnonymousStorageRecord(RecordInfoExtractor extractor){
        this.extractor = extractor;
    }


}
