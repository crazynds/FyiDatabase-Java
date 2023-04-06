package engine.virtualization.record.manager.storage.btree;

import engine.exceptions.DataBaseException;
import engine.file.blocks.ReadableBlock;
import engine.file.streams.ReferenceReadByteStream;
import engine.file.streams.WriteByteStream;
import engine.util.Util;
import lib.btree.BPlusTreeInsertionException;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

public class Page extends Node{

    private static final int HEADERS_SIZE = 4 + 4;

    private int sizeOfPk,sizeOfEntry;

    private int nodes,maxNodes;
    private TreeMap<BigInteger, Map.Entry<Integer,Node>> nodesMap;
    private Map.Entry<Integer,Node> smaller;

    private boolean changed;

    public Page(BTreeHandler handler, int block,Node smaller) {
        super(handler, block);
        this.sizeOfPk=handler.getSizeOfPk();
        this.sizeOfEntry = 4+sizeOfPk;
        this.maxNodes = (getStream().getBlockSize()-HEADERS_SIZE)/sizeOfEntry + 1;
        if(maxNodes <=1 || sizeOfPk<=0) throw new DataBaseException("TreeMap->Page","SizeOfPk é inválido, deve ser maior que 0 caber dentro de um (bloco - headers - 4)");
        this.nodesMap = new TreeMap<>();
        this.nodes = 0;
        this.smaller = null;
        this.changed = true;
        if(smaller!=null){
            this.nodes = 1;
            this.smaller = Map.entry(smaller.block,smaller);
        }
    }

    @Override
    public void save() {
        if(!changed)return;
        WriteByteStream wbs = getStream().getBlockWriteByteStream(block);
        wbs.setPointer(0);
        wbs.writeSeq(new byte[]{2},0,1);
        wbs.writeSeq(Util.convertLongToByteArray(nodes,4),0,4);

        if(nodes>0){
            wbs.writeSeq(Util.convertLongToByteArray(smaller.getKey(),4),0,4);
            Node no =smaller.getValue();
            if(no!=null){
                no.save();
            }
        }
        for (Map.Entry<BigInteger,Map.Entry<Integer,Node>> map:
             nodesMap.entrySet()) {
            int nodeNumber = map.getValue().getKey();
            BigInteger pk = map.getKey();
            Node no =map.getValue().getValue();
            if(no!=null){
                no.save();
            }

            wbs.writeSeq(Util.convertLongToByteArray(nodeNumber,4),0,4);
            wbs.writeSeq(Util.convertNumberToByteArray(pk,sizeOfPk),0,sizeOfPk);
        }

        wbs.commitWrites();
        changed = false;
    }

    @Override
    public void load() {
        ReadableBlock readable = getStream().getBlockReadByteStream(block);

        readable.setPointer(0);
        nodes = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
        if(nodes>maxNodes){
            nodes = (smaller==null)?0:1;
            return;
        }
        ReferenceReadByteStream ref = new ReferenceReadByteStream(readable,readable.getPointer());
        for(int x=0;x<nodes;x++){
            int nodeNumber = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
            if(x==0){
                smaller = Node.<Integer,Node>makeEntry(nodeNumber, (Node) null);
            }else {
                BigInteger pk = getRecordInterface().getPrimaryKey(ref);
                nodesMap.put(
                        pk,
                        Node.<Integer,Node>makeEntry(nodeNumber, (Node) null)
                );
            }
            ref.setOffset(ref.getOffset()+sizeOfEntry);
        }
        changed=false;
    }


    public void insertNode(Node node){
        changed = true;
        if(nodes>=maxNodes){
            throw new DataBaseException("WTF","WTF");
        }
        nodes++;
        BigInteger nodeMin = node.min();
        Node small = loadNodeIfNotExist(smaller);
        if(small.min().compareTo(nodeMin) == -1){
            nodesMap.put(nodeMin,makeEntry(node.block,node));
        }else{
            nodesMap.put(small.min(),smaller);
            smaller = makeEntry(node.block,node);
        }
    }
    public void removeNode(Node node){
        changed = true;
        if(smaller.getValue().block == node.block){
            nodes--;
            smaller = nodesMap.remove(nodesMap.firstKey());
        }else{
            BigInteger key = null;
            for (Map.Entry<BigInteger, Map.Entry<Integer,Node>> val:
                 nodesMap.entrySet()) {
                if(val.getValue().getKey()==node.block){
                    key=val.getKey();
                    break;
                }
            }
            if(key!=null){
                nodes--;
                nodesMap.remove(key);
            }
        }
    }

    @Override
    public void insert(BigInteger t, ByteBuffer m) {
        Node node = findNode(t);
        try{
            node.insert(t,m);
        }catch(BPlusTreeInsertionException exception){
            if(node instanceof Leaf){
                if(node.block != smaller.getKey()){
                    Node small = findNode(node.min().subtract(BigInteger.ONE));
                    BigInteger pk = node.min();
                    ByteBuffer buffer = this.remove(pk);
                    try{
                        small.insert(pk,buffer);
                        // update min key
                        if(node.min().compareTo(t) > 0){
                            node.insert(t,m);
                            removeNode(node);
                            insertNode(node);
                        }else {
                            removeNode(node);
                            insertNode(node);
                            this.insert(t,m);
                        }
                        return;
                    }catch(BPlusTreeInsertionException e2) {
                        node.insert(pk,buffer);
                    }
                }
                if(node.block != nodesMap.get(nodesMap.lastKey()).getKey()){
                    Node bigger = findNode(nodesMap.higherKey(node.min()));
                    BigInteger pk = node.max();
                    ByteBuffer buffer = this.remove(pk);
                    try{
                        bigger.insert(pk,buffer);

                        removeNode(bigger);
                        insertNode(bigger);

                        this.insert(t,m);
                        return;
                    }catch(BPlusTreeInsertionException e2) {
                        node.insert(pk,buffer);
                    }
                }
            }

            // Se necessário faz o split dos dados
            if(isFull())
                throw exception;
            Node rigth = node.half();
            insertNode(rigth);
            this.insert(t,m);
        }
        changed=true;
    }

    @Override
    public ByteBuffer remove(BigInteger t) {
        Node node = findNode(t);
        changed = true;
        BigInteger minNode = node.min();
        ByteBuffer buff = node.remove(t);

        if(!node.hasMinimun()){
            if(node.block==smaller.getKey()){
                Node bigger = findNode(nodesMap.firstKey());
                removeNode(bigger);

                BigInteger min = bigger.min();
                ByteBuffer buff2 = bigger.remove(min);
                if(bigger.hasMinimun()){
                    node.insert(min,buff2);
                    insertNode(bigger);
                }else {
                    node.merge(bigger);
                    node.insert(min,buff2);
                    handler.getBlockManager().free(bigger.block);
                }
            }else{
                Node small = findNode(minNode.subtract(BigInteger.ONE));

                BigInteger max = small.max();
                ByteBuffer buff2 = small.remove(max);

                removeNode(node);

                if(small.hasMinimun()){
                    node.insert(max,buff2);
                    insertNode(node);
                }else {
                    small.merge(node);
                    small.insert(max,buff2);
                    handler.getBlockManager().free(node.block);
                }
            }
        }

        return buff;
    }

    private Node findNode(BigInteger t){
        Map.Entry<BigInteger, Map.Entry<Integer, Node>> entry = nodesMap.floorEntry(t);
        Map.Entry<Integer, Node> node;
        if(entry == null)
            node = smaller;
        else node = entry.getValue();
        return loadNodeIfNotExist(node);
    }

    private Node loadNodeIfNotExist(Map.Entry<Integer, Node> node){
        if(node.getValue()==null){
            Node n =loadNode(node.getKey());
            node.setValue(n);
        }
        return node.getValue();
    }

    @Override
    public ByteBuffer get(BigInteger t) {
        Node node = findNode(t);
        return node.get(t);
    }

    @Override
    protected Node half() {
        Vector<Map.Entry<BigInteger, Map.Entry<Integer,Node>>> vet = new Vector<>();
        int x = 0;
        for (Map.Entry<BigInteger, Map.Entry<Integer,Node>> e:
             this.nodesMap.entrySet()) {
            if(x>=this.nodesMap.size()/2){
                vet.add(makeEntry(e.getKey(),e.getValue()));
            }
            x++;
        }
        for(Map.Entry<BigInteger, Map.Entry<Integer,Node>> e:vet){
            this.nodesMap.remove(e.getKey());
            this.nodes--;
        }
        Page rigth = new Page(this.handler,this.handler.getBlockManager().allocNew(),vet.get(0).getValue().getValue());
        for(int y=1;y<vet.size();y++){
            rigth.insertNode(vet.get(y).getValue().getValue());
        }
        return rigth;
    }

    @Override
    public Node merge(Node node) {
        Page leaf = (Page)node;
        for (Map.Entry<BigInteger, Map.Entry<Integer,Node>> val:
                nodesMap.entrySet()) {
            nodesMap.put(val.getKey(),val.getValue());
        }
        return leaf;
    }

    @Override
    public void print(int tabs) {
        for(int x=0;x<tabs;x++)System.out.print("\t");
        System.out.println("V");
        for (int x = 0; x < tabs; x++) System.out.print("\t");
        System.out.println(0+"-> BLOCO: "+smaller.getKey());
        smaller.getValue().print(tabs+1);
        for (Map.Entry<BigInteger, Map.Entry<Integer, Node>> e:nodesMap.entrySet()) {
            Node n = loadNodeIfNotExist(e.getValue());
            for (int x = 0; x < tabs; x++) System.out.print("\t");
            System.out.println(e.getKey()+"-> BLOCO: "+e.getValue().getKey());
            n.print(tabs+1);
        }
    }

    @Override
    public boolean hasMinimun() {
        return  nodesMap.size()>=maxNodes/2;
    }

    @Override
    public boolean isFull() {
        return nodes>=maxNodes;
    }

    @Override
    public BigInteger min() {
        return loadNodeIfNotExist(smaller).min();
    }

    @Override
    public BigInteger max() {
        if(nodes<=1)return loadNodeIfNotExist(smaller).max();
        return loadNodeIfNotExist(nodesMap.lastEntry().getValue()).max();
    }

    @Override
    public int height() {
        return loadNodeIfNotExist(smaller).height()+1;
    }

    @Override
    public Leaf leafFrom(BigInteger key) {
        Node node = findNode(key);
        return node.leafFrom(key);
    }

    @Override
    public Iterator<Map.Entry<BigInteger, ByteBuffer>> iterator() {
        return this.iterator(min());
    }
    public Iterator<Map.Entry<BigInteger, ByteBuffer>> iterator(BigInteger pk) {
        return new Iterator<Map.Entry<BigInteger, ByteBuffer>>() {

            Iterator<Map.Entry<Integer,Node>> itNodes = nodesMap.values().iterator();
            Iterator<Map.Entry<BigInteger, ByteBuffer>> currIt = null;

            Iterator<Map.Entry<BigInteger, ByteBuffer>> getCurrIt(){
                while((currIt==null || !currIt.hasNext()) && itNodes.hasNext()){
                    Map.Entry<Integer,Node> e = itNodes.next();
                    Node n = loadNodeIfNotExist(e);
                    currIt = n.iterator(pk);
                }
                return currIt;
            }

            @Override
            public boolean hasNext() {
                Iterator<Map.Entry<BigInteger, ByteBuffer>> a = getCurrIt();
                if(a==null)return false;
                return true;
            }

            @Override
            public Map.Entry<BigInteger, ByteBuffer> next() {
                Iterator<Map.Entry<BigInteger, ByteBuffer>> a = getCurrIt();
                if(a==null)return null;
                return a.next();
            }
        };
    }
}
