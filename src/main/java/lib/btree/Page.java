package lib.btree;

import java.util.*;
import java.util.stream.Collectors;

public class Page<T extends Comparable<T>,M> extends Node<T,M> {

    //Se for menor que essa key então é o node dessa entrada

    protected TreeMap<T,Node<T,M>> nodes;

    protected T actualMin;

    public Page(Node<T,M> children){
        nodes = new TreeMap<>();
        nodes.put(children.min(),children);
        actualMin = children.min();
    }

    protected void insertNode(Node<T,M> node){
        nodes.put(node.min(),node);
        actualMin = node.min();
    }

    private Map.Entry<T,Node<T,M>> findNode(T key){
        Map.Entry<T,Node<T,M>> entry = nodes.lowerEntry(key);
        if(entry == null)return nodes.firstEntry();
        return entry;
    }

    @Override
    public Map.Entry<T,M> insert(T t, M m) {
        Map.Entry<T,Node<T,M>> entry = findNode(t);
        Node<T,M> node = entry.getValue();
        Map.Entry<T,M> e = node.insert(t, m);
        if(e==null){
            if (t.compareTo(actualMin) == -1) actualMin = t;
            nodes.remove(entry.getKey());
            nodes.put(node.min(),node);
        }else{
            t = (T)e.getKey();
            m = (M)e.getValue();

            if(nodes.size() < BPlusTree.ORDER){
                Node<T,M> right = node.half();
                insertNode(right);
                insert(t,m);
                return null;
            }
            // se nada pode acontecer, throw pra cima
            return e;
        }
        return null;
    }

    @Override
    public M get(T t) {
        return findNode(t).getValue().get(t);
    }

    @Override
    public M remove(T t) {
        Node<T,M> node = findNode(t).getValue();
        M removed = node.remove(t);
        if(t.compareTo(actualMin)==0)actualMin=node.min();

        //Se um nó não tem um minimo, remove ele da filiação dos nós, e itera entre os valores dentro dele para dentro dos outros nós, se necessário vai criar novos nós
        if(!node.hasMinimun()){
            Node<T,M> no = nodes.remove(t);
            //para cada entrada do nó removido insere normalmente na arvore
            for (Map.Entry<T,M> entry:
                 no) {
                insert(entry.getKey(), entry.getValue());
            }
            // executa ação para liberar esse nó
            // No caso é so ignorar que o nó vai ser limpo sozinho
        }
        return removed;
    }

    @Override
    public Node<T, M> half() {
        T val = actualMin;
        int x=0;
        for(T node:nodes.keySet()){
            val = node;
            x++;
            if(x>=nodes.size()/2)break;
        }
        SortedMap<T,Node<T,M>> subNodes = nodes.subMap(val,nodes.lastKey());
        Page<T,M> rigth = null;
        ArrayList<T> list = new ArrayList<>();
        for (Map.Entry<T,Node<T,M>> n:
                subNodes.entrySet()) {
            if(rigth==null){
                rigth = new Page<>(n.getValue());
            }else{
                rigth.insertNode(n.getValue());
                list.add(n.getKey());
            }
        }
        for(T node:list){
            nodes.remove(node);
        }
        actualMin = nodes.firstEntry().getValue().min();
        return rigth;
    }

    @Override
    public Node<T, M> merge(Node<T, M> node) {
        return null;
    }

    @Override
    public boolean hasMinimun() {
        return nodes.size() >= BPlusTree.ORDER/2;
    }

    @Override
    public boolean isFull() {
        if(nodes.size()<BPlusTree.ORDER)return false;
        //Se tiver alguma casa que está cheia e os caras ao lado também estão cheios, então retornar que esse nó está cheio
        boolean leftFull = false;
        boolean midFull = true;
        boolean rightFull = nodes.firstEntry().getValue().isFull();
        for(Node<T,M> no:
                nodes.values()){
            leftFull = midFull;
            midFull = rightFull;
            rightFull = no.isFull();
            if (midFull==true && leftFull == true && rightFull == true)return true;
        }
        leftFull = midFull;
        midFull = rightFull;
        rightFull = true;
        if (midFull==true && leftFull == true && rightFull == true)return true;
        return false;
    }

    @Override
    public T min() {
        //return nodes.get(0).min();
        return actualMin;
    }

    @Override
    public T max() {
        return nodes.lastEntry().getValue().max();
    }

    @Override
    public int height() {
        return nodes.firstEntry().getValue().height()+1;
    }

    @Override
    public void print(int tabs) {
        for(int x=0;x<tabs;x++)System.out.print("\t");
        System.out.println("V");
        int y=0;
        for (Node<T,M> no:
            nodes.values()) {
            no.print(tabs+1);
            for (int x = 0; x < tabs; x++) System.out.print("\t");
            System.out.println(no.min()+"->");
            y++;
        }
    }

    @Override
    public Leaf<T, M> leafFrom(T key) {
        return findNode(key).getValue().leafFrom(key);
    }

    @Override
    public Iterator<Map.Entry<T, M>> iterator() {
        return new Iterator<Map.Entry<T, M>>() {

            Iterator<Node<T,M>> nodeIterator = nodes.values().iterator();
            Iterator<Map.Entry<T,M>> iterator = null;

            @Override
            public boolean hasNext() {
                while(iterator==null || !iterator.hasNext()) {
                    if (!nodeIterator.hasNext()) return false;
                    iterator = nodeIterator.next().iterator();
                }
                return true;
            }

            @Override
            public Map.Entry<T, M> next() {
                if(hasNext())
                    return iterator.next();
                return null;
            }
        };
    }
}
