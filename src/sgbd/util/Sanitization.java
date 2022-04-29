package sgbd.util;

import sgbd.query.Tuple;

public interface Sanitization {

    public Tuple sanitize(Tuple t);
}
