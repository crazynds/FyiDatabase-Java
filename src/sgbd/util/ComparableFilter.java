package sgbd.util;

public interface ComparableFilter<T> {
    public boolean match(T t1,T t2);
}
