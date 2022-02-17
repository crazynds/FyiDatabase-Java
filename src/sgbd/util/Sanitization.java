package sgbd.util;

import sgbd.query.basic.Tuple;

public interface Sanitization {

    public Tuple sanitize(Tuple t);
}
