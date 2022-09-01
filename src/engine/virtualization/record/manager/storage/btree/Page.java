package engine.virtualization.record.manager.storage.btree;

import com.sun.source.tree.Tree;
import engine.exceptions.DataBaseException;
import engine.file.blocks.BlockID;
import engine.file.blocks.ReadableBlock;
import engine.file.buffers.BlockBuffer;
import engine.file.streams.ReferenceReadByteStream;
import engine.file.streams.WriteByteStream;
import engine.util.Util;
import engine.virtualization.record.RecordInterface;
import lib.btree.BPlusTreeInsertionException;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class Page extends Node{

    private static final int HEADERS_SIZE = 4 + 4;

    private int sizeOfPk,sizeOfEntry;

    private int nodes,maxNodes;
    private TreeMap<BigInteger, Map.Entry<Integer,Node>> nodesMap;
    private Map.Entry<Integer,Node> smaller;

    private boolean changed = false;

    public Page(BTreeHandler handler, int block,Node smaller) {
        super(handler, block);
        this.sizeOfPk=handler.getSizeOfPk();
        this.sizeOfEntry = 4+sizeOfPk;
        this.maxNodes = (getStream().getBlockSize()-HEADERS_SIZE)/sizeOfEntry + 1;
        if(maxNodes <=1 || sizeOfPk<=0) throw new DataBaseException("TreeMap->Page","SizeOfPk é inválido, deve ser maior que 0 caber dentro de um (bloco - headers - 4)");
        this.nodesMap = new TreeMap<>();
        this.nodes = 0;
        this.smaller = null;
        if(smaller!=null){
            this.nodes = 1;
            this.smaller = Map.entry(smaller.block,smaller);
        }
    }

    @Override
    public void save() {
        if(!changed)return;
        ReadableBlock readable = getStream().getBlockReadByteStream(block);
        WriteByteStream wbs = getStream().getBlockWriteByteStream(block);
        wbs.setPointer(0);
        wbs.writeSeq(new byte[]{1},0,1);
        wbs.writeSeq(Util.convertLongToByteArray(nodes,4),0,4);

        wbs.writeSeq(Util.convertLongToByteArray(smaller.getKey(),4),0,4);
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
        ReferenceReadByteStream ref = new ReferenceReadByteStream(readable,readable.getPointer());
        for(int x=0;x<nodes;x++){
            int nodeNumber = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
            if(x==0){
                smaller = Map.entry(nodeNumber, (Node) null);
            }else {
                BigInteger pk = getRecordInterface().getPrimaryKey(ref);
                nodesMap.put(
                        pk,
                        Map.entry(nodeNumber, (Node) null)
                );
            }
            ref.setOffset(ref.getReference()+sizeOfEntry);
        }
        changed=false;
    }


    public void insertNode(Node node){
        nodes++;
        BigInteger nodeMin = node.min();
        Node small = loadNodeIfNotExist(smaller);
        if(small.min().compareTo(nodeMin) == -1){
            nodesMap.put(nodeMin,Map.entry(node.block,node));
        }else{
            nodesMap.put(small.min(),smaller);
            smaller = Map.entry(node.block,node);
        }
    }

    @Override
    public void insert(BigInteger t, ByteBuffer m) {
        Node node = findNode(t);
        try{
            node.insert(t,m);
        }catch(BPlusTreeInsertionException exception){
            if(isFull())
                throw exception;

        }
        changed=true;
    }

    @Override
    public ByteBuffer remove(BigInteger t) {
        Node node = findNode(t);
        changed = true;
        return node.remove(t);
    }

    private Node findNode(BigInteger t){
        Map.Entry<BigInteger, Map.Entry<Integer, Node>> entry = nodesMap.ceilingEntry(t);
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
        return null;
    }

    @Override
    public Node merge(Node node) {
        return null;
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
        return nodesMap.size()>=maxNodes;
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

}
