package sgbd.prototype.query;

import java.util.*;

public class OperatorSchema implements Iterable<Map.Entry<String,Fields>> {

    private HashMap<String,Fields> map;

    public OperatorSchema(){
        map = new HashMap<>();
    }


    @Override
    public Iterator<Map.Entry<String, Fields>> iterator() {
        return map.entrySet().iterator();
    }
}
