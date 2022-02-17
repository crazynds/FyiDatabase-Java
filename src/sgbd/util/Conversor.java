package sgbd.util;

import sgbd.prototype.Column;
import sgbd.query.basic.Tuple;

public interface Conversor {

    public Column metaInfo(Tuple t);

    public byte[] process(Tuple t);


}
