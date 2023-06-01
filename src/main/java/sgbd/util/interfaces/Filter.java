package sgbd.util.interfaces;

public interface Filter<T>{

    public boolean match(T t);

}
