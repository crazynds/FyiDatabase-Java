package sgbd.util.interfaces;

import sgbd.prototype.column.Column;
import sgbd.prototype.query.Tuple;

public interface Conversor {

    public Column metaInfo(Tuple t);

    public byte[] process(Tuple t);


}
