package sgbd.util;

public interface Filter<T>{

    public boolean match(T t);

}
