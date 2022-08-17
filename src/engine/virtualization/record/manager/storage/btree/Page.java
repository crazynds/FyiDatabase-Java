package engine.virtualization.record.manager.storage.btree;

import com.sun.source.tree.Tree;
import engine.file.blocks.BlockID;
import engine.file.buffers.BlockBuffer;
import engine.virtualization.record.RecordInterface;
import lib.btree.BPlusTreeInsertionException;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class Page extends Node{

    private int sizeOfPk;
    private int nodes,maxNodes;
    private TreeMap<BigInteger, Map.Entry<Integer,Node>> nodesMap;
    private Map.Entry<Integer,Node> smaller;

    public Page(BlockBuffer stream, RecordInterface ri, int block, int sizeOfPk) {
        super(stream, ri, block);
        this.sizeOfPk=sizeOfPk;

    }

    @Override
    public void save() {

    }

    @Override
    public void load() {

    }


    @Override
    public void insert(BigInteger t, ByteBuffer m) {
        try{


        }catch(BPlusTreeInsertionException exception){

        }
    }

    private Node loadNode(Map.Entry<Integer, Node> nodeInfo){

        /**
         * Aqui o node.getValue pode ser null ainda,
         * então pegar o int dele e carregar o node correspondente
         */

        return nodeInfo.getValue();
    }

    private Node findNode(BigInteger t){
        Map.Entry<BigInteger, Map.Entry<Integer, Node>> entry = nodesMap.floorEntry(t);
        Map.Entry<Integer, Node> node;
        if(entry == null)
            node = smaller;
        else node = entry.getValue();

        return loadNode(node);
    }

    @Override
    public ByteBuffer get(BigInteger t) {
        Node node = findNode(t);
        return node.get(t);
    }

    @Override
    public ByteBuffer remove(BigInteger t) {
        Node node = findNode(t);
        return node.remove(t);
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
    public boolean hasMinimun() {
        return  nodesMap.size()>=maxNodes/2;
    }

    @Override
    public boolean isFull() {
        return nodesMap.size()>=maxNodes;
    }

    @Override
    public BigInteger min() {
        return loadNode(smaller).min();
    }

    @Override
    public BigInteger max() {
        return loadNode(nodesMap.lastEntry().getValue()).max();
    }

    @Override
    public int height() {
        return loadNode(smaller).height()+1;
    }

    @Override
    public Leaf leafFrom(BigInteger key) {
        Node node = findNode(key);
        return node.leafFrom(key);
    }

}
