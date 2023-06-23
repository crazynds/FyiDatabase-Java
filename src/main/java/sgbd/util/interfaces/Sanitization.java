package sgbd.util.interfaces;

import sgbd.prototype.query.Tuple;

public interface Sanitization {

    public Tuple sanitize(Tuple t);
}
