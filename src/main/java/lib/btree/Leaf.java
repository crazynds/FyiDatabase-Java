package lib.btree;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class Leaf<T extends Comparable<T>,M> extends Node<T,M>{
    private Map.Entry<T,M>[] itens;
    private int qtd;
    private Leaf nextLeaf;

    public Leaf(){
        itens = new Map.Entry[BPlusTree.ORDER];
        qtd = 0;
    }

    public Leaf getNextLeaf(){
        return nextLeaf;
    }
    public void setNextLeaf(Leaf next){
        this.nextLeaf = next;
    }

    public int size(){
        return qtd;
    }
    public T min(){
        return itens[0].getKey();
    }
    public T max(){
        return itens[qtd-1].getKey();
    }
    public M get(T t){
        int max = qtd-1;
        int min = 0;
        while(min<=max){
            int med = min+ (max-min)/2;
            T key = itens[med].getKey();
            M data = itens[med].getValue();
            switch(t.compareTo(key)){
                case 0:
                    return data;
                case 1:
                    min = med+1;
                    break;
                case -1:
                    max = med-1;
                    break;
            }
        }
        return null;
    }
    public M remove(T key){
        int x;
        M data = null;
        for(x=0;x<qtd;x++) {
            T k = itens[x].getKey();
            M d = itens[x].getValue();
            if (key.compareTo(k) == 0) {
                data = d;
                itens[x] = null;
                break;
            }
        }
        for(x+=1;x<qtd;x++){
            itens[x-1] = itens[x];
        }
        if(data!=null)qtd--;
        return data;
    }

    @Override
    protected Node<T, M> half() {
        Leaf<T,M> left = this;
        Leaf<T,M> right = new Leaf<>();

        int savedQtd = qtd;

        for(int x=qtd/2;x<savedQtd;x++){
            Map.Entry<T,M> aux = itens[x];
            right.insert(aux.getKey(),aux.getValue());
            itens[x] = null;
            qtd--;
        }
        right.setNextLeaf(left.getNextLeaf());
        left.setNextLeaf(right);
        return right;
    }

    @Override
    public Node<T, M> merge(Node<T, M> node) {
        return null;
    }

    @Override
    public boolean hasMinimun() {
        return qtd>= BPlusTree.ORDER/2;
    }

    @Override
    public boolean isFull() {
        return qtd>=itens.length;
    }

    @Override
    public int height() {
        return 0;
    }

    public Map.Entry<T,M> insert(T key, M data){
        Map.Entry<T,M> entry = new AbstractMap.SimpleEntry<>(key,data);
        if(qtd == itens.length && get(key)==null)
            return entry;
        int min= 0;
        int max = qtd;
        while(min<max){
            int med = min+ (max-min)/2;
            T t = itens[med].getKey();
            switch(key.compareTo(t)){
                case 0:
                    min = max = med;
                    break;
                case 1:
                    min = med+1;
                    break;
                case -1:
                    max = med-1;
                    break;
            }
        }
        for(int x=min;x<qtd;x++){
            T k = itens[x].getKey();
            M d = itens[x].getValue();
            switch (key.compareTo(k)){
                case 0:
                    itens[x].setValue(data);
                    return null;
                case -1:
                    Map.Entry<T,M> aux = itens[x];
                    itens[x] = entry;
                    entry = aux;
                    break;
            }
        }
        if(entry!=null){
            itens[qtd] = entry;
            qtd++;
        }
        return null;
    }

    @Override
    public void print(int tabs) {
        for(int x=0;x<tabs;x++)System.out.print("\t");
        System.out.println(Arrays.toString(Arrays.stream(itens).map(tmEntry -> tmEntry!=null?tmEntry.getKey() + "-"+ tmEntry.getValue():"null").toArray()));
    }

    @Override
    public Leaf<T, M> leafFrom(T key) {
        return this;
    }

    @Override
    public Iterator<Map.Entry<T, M>> iterator() {
        return Arrays.stream(itens).limit(qtd).iterator();
    }

    public Iterator<Map.Entry<T, M>> iterator(T key) {
        return Arrays.stream(itens).limit(qtd).filter((entry)->entry.getKey().compareTo(key)>=0).iterator();
    }
}
