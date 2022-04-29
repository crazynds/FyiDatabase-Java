package lib.btree;

import java.util.Map;

public abstract class Node<T extends Comparable<T>,M>  implements Iterable<Map.Entry<T,M>> {

    public abstract void insert(T t,M m);
    public abstract M get(T t);
    public abstract M remove(T t);

    //Pega metade dos dados do nó atual e retorna um novo nó com dados do antigo nó
    protected abstract Node<T,M> half();
    public abstract Node<T,M> merge(Node<T,M> node);


    public abstract boolean hasMinimun();

    public abstract boolean isFull();
    public abstract T min();
    public abstract T max();


    public abstract int height();

    public abstract void print(int tabs);


    public abstract Leaf<T,M> leafFrom(T key);

}
