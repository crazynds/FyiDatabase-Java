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

    private int sizeOfPk;
    private int nodes,maxNodes,sizeOfEntry;
    private TreeMap<BigInteger, Map.Entry<Integer,Node>> nodesMap;
    private Map.Entry<Integer,Node> smaller;

    private static final int HEADERS_SIZE = 4 + 4;

    public Page(BlockBuffer stream, RecordInterface ri,BTreeHandler handler, int block) {
        super(stream, ri, handler, block);
        this.sizeOfPk=handler.getSizeOfPk();
        this.sizeOfEntry = 4+sizeOfPk;
        this.maxNodes = (stream.getBlockSize()-HEADERS_SIZE)/sizeOfEntry + 1;
        this.nodes = 0;
        this.nodesMap = new TreeMap<>();
        this.smaller = null;
        if(maxNodes <=1 || sizeOfPk<=0) throw new DataBaseException("TreeMap->Page","SizeOfPk é inválido, deve ser maior que 0 caber dentro de um (bloco - headers - 4)");
    }

    @Override
    public void save() {
        ReadableBlock readable = stream.getBlockReadByteStream(block);
        WriteByteStream wbs = stream.getBlockWriteByteStream(block);
        wbs.setPointer(0);
        wbs.writeSeq(Util.convertLongToByteArray(nodes,4),0,4);

        wbs.writeSeq(Util.convertLongToByteArray(smaller.getKey(),4),0,4);
        for (Map.Entry<BigInteger,Map.Entry<Integer,Node>> map:
             nodesMap.entrySet()) {
            int nodeNumber = map.getValue().getKey();
            BigInteger pk = map.getKey();

            wbs.writeSeq(Util.convertLongToByteArray(nodeNumber,4),0,4);
            wbs.writeSeq(Util.convertNumberToByteArray(pk,sizeOfPk),0,sizeOfPk);
        }

        wbs.commitWrites();
    }

    @Override
    public void load() {
        ReadableBlock readable = stream.getBlockReadByteStream(block);

        readable.setPointer(0);
        nodes = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
        ReferenceReadByteStream ref = new ReferenceReadByteStream(readable,readable.getPointer());
        for(int x=0;x<nodes;x++){
            int nodeNumber = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
            if(x==0){
                smaller = Map.entry(nodeNumber, (Node) null);
            }else {
                BigInteger pk = ri.getExtractor().getPrimaryKey(ref);
                nodesMap.put(
                        pk,
                        Map.entry(nodeNumber, (Node) null)
                );
            }
            ref.setOffset(ref.getReference()+sizeOfEntry);
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
