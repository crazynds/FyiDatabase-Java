package lib.btree;

import java.util.*;
import java.util.stream.Collectors;

public class Page<T extends Comparable<T>,M> extends Node<T,M> {

    //Se for menor que essa key então é o node dessa entrada

    protected ArrayList<T> keys;
    protected ArrayList<Node<T,M>> nodes;

    protected T actualMin;

    public Page(Node<T,M> children){
        keys = new ArrayList<>(BPlusTree.ORDER-1);
        nodes = new ArrayList<>(BPlusTree.ORDER);
        nodes.add(children);
        actualMin = children.min();
    }

    protected void insertNode(Node<T,M> node){
        nodes.add(node);
        keys.add(node.min());
        keys.add(nodes.get(0).min());

        nodes.sort(Comparator.comparing(Node::min));
        keys.sort(Comparator.comparing(t -> t));

        actualMin = keys.remove(0);

    }

    private int findNode(T key){
        for (T t:
                keys) {
            if(key.compareTo(t)==-1){
                return keys.indexOf(t);
            }
        }
        return keys.size();
    }
    private void freeToLeft(int index, T t, M m){
        T auxT=nodes.get(index).min();
        M auxM=nodes.get(index).remove(auxT);

        nodes.get(index-1).insert(auxT,auxM);

        // remove a key minima do cara atual, pq ela vai mudar
        keys.remove(index-1);
        // insere o novo valor
        nodes.get(index).insert(t,m);
        // busca a nova key minima
        keys.add(nodes.get(index).min());
        // Ordena novamente as keys, não é necessário ordenar os nodos
        keys.sort(Comparator.comparing(ta -> ta));
    }
    private void freeToRight(int index, T t, M m){
        T auxT=nodes.get(index).max();
        // remove a key minima do cara da direita, pq ele recebeu um novo minimo
        keys.remove(index);

        // Se o valor que vai ser inserido é maior que o valor que vai ser movido,
        // não é necessario remover o da esquerda, somente mandar o cara atual pra direita
        if(auxT.compareTo(t)!=1){
            nodes.get(index+1).insert(t,m);
            keys.add(t);
        }else{
            M auxM=nodes.get(index).remove(auxT);
            nodes.get(index+1).insert(auxT,auxM);
            nodes.get(index).insert(t,m);
            keys.add(auxT);
        }

        // Ordena novamente as chaves
        keys.sort(Comparator.comparing(ta -> ta));

        if (t.compareTo(actualMin) == -1)
            actualMin = t;
    }

    @Override
    public void insert(T t, M m) {
        int index = findNode(t);
        try {
            nodes.get(index).insert(t, m);
            if (t.compareTo(actualMin) == -1) actualMin = t;
        }catch (BPlusTreeInsertionException e){
            t = (T)e.getEntry().getKey();
            m = (M)e.getEntry().getValue();

            if(nodes.size() < BPlusTree.ORDER){
                Node<T,M> right = nodes.get(index).half();
                insertNode(right);
                insert(t,m);
                return;
            }
            // se nada pode acontecer, throw pra cima
            throw e;
        }
    }

    @Override
    public M get(T t) {
        int index = findNode(t);
        return nodes.get(index).get(t);
    }

    @Override
    public M remove(T t) {
        int index = findNode(t);
        M removed = nodes.get(index).remove(t);
        if(t.compareTo(actualMin)==0)actualMin=nodes.get(0).min();

        //Se um nó não tem um minimo, remove ele da filiação dos nós, e itera entre os valores dentro dele para dentro dos outros nós, se necessário vai criar novos nós
        if(!nodes.get(index).hasMinimun()){
            Node<T,M> no = nodes.remove(index);
            keys.remove((index==0)?index:index-1);
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
        List<Node<T,M>> newNodes = nodes.subList(nodes.size()/2,nodes.size());
        Page<T,M> rigth = null;
        for (Node<T,M> n:
                newNodes) {
            if(rigth==null){
                rigth = new Page<>(n);
            }else rigth.insertNode(n);
        }

        nodes.removeAll(newNodes);
        keys.clear();
        keys.addAll(nodes.stream().map(no->no.min()).collect(Collectors.toList()));
        keys.sort(Comparator.comparing(t->t));
        actualMin = keys.remove(0);


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
        boolean leftFull = true;
        boolean midFull = nodes.get(0).isFull();
        boolean rightFull = nodes.get(1).isFull();
        if (midFull==true && leftFull == true && rightFull == true)return true;
        for(int x=2;x<nodes.size();x++){
            leftFull = midFull;
            midFull = rightFull;
            rightFull = nodes.get(x).isFull();
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
        return nodes.get(keys.size()).max();
    }

    @Override
    public int height() {
        return nodes.get(0).height()+1;
    }

    @Override
    public void print(int tabs) {
        for(int x=0;x<tabs;x++)System.out.print("\t");
        System.out.println("V");
        int y=0;
        for (Node<T,M> no:
             nodes) {
            no.print(tabs+1);
            if(y<keys.size()) {
                for (int x = 0; x < tabs; x++) System.out.print("\t");
                System.out.println(keys.get(y)+"->");
            }
            y++;
        }
    }

    @Override
    public Leaf<T, M> leafFrom(T key) {
        return nodes.get(findNode(key)).leafFrom(key);
    }

    @Override
    public Iterator<Map.Entry<T, M>> iterator() {
        return new Iterator<Map.Entry<T, M>>() {

            Iterator<Node<T,M>> nodeIterator = nodes.iterator();
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
