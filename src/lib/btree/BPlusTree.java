package lib.btree;

import sgbd.util.Faker;

import java.security.InvalidParameterException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class BPlusTree<T extends Comparable<T>,M>  implements Iterable<Map.Entry<T,M>>{

    protected static final int ORDER  = 4;

    private Node<T,M> rootNode;
    private Leaf<T,M> firstLeaf;

    public static void main(String[] args){
        BPlusTree<Integer,String> bee = new BPlusTree<>();

        Faker.replaceRandom(new Random(100));
        for (int x=0;x<1000;x++){
            String name = Faker.firstName();
            int id = Faker.integer(1,1000);
            bee.insert(id,name);
        }

        Integer oldValue = -1;
        for(Map.Entry<Integer,String> itens:bee){
            System.out.println(itens.getKey()+"-"+itens.getValue());
            if(itens.getKey()<oldValue){
                System.out.println("!Analisar aq q n ta certo!");
            }
            oldValue = itens.getKey();
        }
    }

    public BPlusTree(){
        firstLeaf = new Leaf();
        rootNode = firstLeaf;
        if(ORDER<2)throw new InvalidParameterException();
    }

    public void clear(){
        firstLeaf = new Leaf();
        rootNode = firstLeaf;
    }


    public void insert(T t, M d){
        try {
            rootNode.insert(t, d);
        }catch (BPlusTreeInsertionException e){
            Node<T,M> left = rootNode;
            Node<T,M> right = rootNode.half();
            Page<T,M> page = new Page<>(left);
            page.insertNode(right);
            rootNode = page;
            this.insert((T)e.getEntry().getKey(),(M)e.getEntry().getValue());
        }
    }
    public M get(T t){
        return rootNode.get(t);
    }
    public M remove(T t){
        return rootNode.remove(t);
    }

    public void print(){
        rootNode.print(0);
    }



    public Iterator<Map.Entry<T, M>> iterator(T key) {
        return new Iterator<Map.Entry<T, M>>() {

            Leaf<T,M> actualLeaf = rootNode.leafFrom(key);

            Iterator<Map.Entry<T,M>> iterator = null;

            private boolean loadNextIterator(){
                if(actualLeaf==null)return false;
                if(iterator==null){
                    iterator = actualLeaf.iterator(key);
                    int count = 0;
                    while(iterator.hasNext() && iterator.next().getKey().compareTo(key)==-1){
                        count++;
                    };
                    if(iterator.hasNext()) {
                        iterator = actualLeaf.iterator();
                        for (int x = 0; x < count; x++) iterator.next();
                    }
                }else{
                    iterator = actualLeaf.iterator(key);
                }
                actualLeaf = actualLeaf.getNextLeaf();
                return true;
            }

            @Override
            public boolean hasNext() {
                while((iterator==null || !iterator.hasNext()) && loadNextIterator());

                if(iterator==null)return false;
                return iterator.hasNext();
            }

            @Override
            public Map.Entry<T, M> next() {
                if(!hasNext()) return null;
                return iterator.next();
            }
        };
    }


    @Override
    public Iterator<Map.Entry<T, M>> iterator() {
        //Easy way, but is to low efficient
        //return rootNode.iterator();

        //Hard way, but is efficient
        return new Iterator<Map.Entry<T, M>>() {

            Leaf<T,M> actualLeaf = firstLeaf;

            Iterator<Map.Entry<T,M>> iterator = null;

            private boolean loadNextIterator(){
                if(actualLeaf==null)return false;
                iterator = actualLeaf.iterator();
                actualLeaf = actualLeaf.getNextLeaf();
                return true;
            }

            @Override
            public boolean hasNext() {
                while((iterator==null || !iterator.hasNext()) && loadNextIterator());

                if(iterator==null)return false;
                return iterator.hasNext();
            }

            @Override
            public Map.Entry<T, M> next() {
                if(!hasNext()) return null;
                return iterator.next();
            }
        };
    }
}
