package lib.btree;

import java.util.Map;

public class BPlusTreeInsertionException extends RuntimeException {

    private Map.Entry entry;

    public BPlusTreeInsertionException(Map.Entry<?, ?> entry){
        this.entry = entry;
    }

    public Map.Entry<?,?> getEntry(){
        return entry;
    }

}
