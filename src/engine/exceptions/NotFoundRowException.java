package engine.exceptions;

import java.math.BigInteger;

public class NotFoundRowException extends DataBaseException{
    public NotFoundRowException(String locale, BigInteger pk) {
        super(locale, "Record com a primary key de valor "+pk.longValue()+" não foi encontrado.");
    }
}
