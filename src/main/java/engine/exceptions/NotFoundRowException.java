package engine.exceptions;


import lib.BigKey;

public class NotFoundRowException extends DataBaseException{
    public NotFoundRowException(String locale, BigKey pk) {
        super(locale, "Record com a primary key de valor "+pk.longValue()+" não foi encontrado.");
    }
}
