package engine.storage.sorted;

import engine.storage.common.AnonymousStorageRecord;
import engine.virtualization.record.RecordInfoExtractor;

import java.math.BigInteger;

public abstract class SortedAnonymousStorageRecord extends PkStorageRecord<BigInteger> {

    protected RecordInfoExtractor extractor;


    public SortedAnonymousStorageRecord(RecordInfoExtractor extractor){
        this.extractor = extractor;
    }


}
