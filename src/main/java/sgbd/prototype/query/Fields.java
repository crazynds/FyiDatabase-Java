package sgbd.prototype.query;

import sgbd.prototype.column.Column;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Fields implements Iterable<Map.Entry<String, Column>> {

    private HashMap<String,Column> map = new HashMap<>();

    @Override
    public Iterator<Map.Entry<String, Column>> iterator() {
        return map.entrySet().iterator();
    }
}
