package engine.virtualization.record.manager.storage.btree;

import engine.exceptions.DataBaseException;
import engine.file.blocks.ReadableBlock;
import engine.file.buffers.BlockBuffer;
import engine.file.streams.BlockStream;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordInterface;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public abstract class Node implements Iterable<Map.Entry<BigInteger,ByteBuffer>>{

    protected int block;
    protected BTreeHandler handler;

    public Node(BTreeHandler handler, int block){
        this.block = block;
        this.handler=handler;
        this.handler.getBlockManager().setNode(block);
        this.handler.nodeRelation.put(block,this);
    }

    public Node loadNode(int blockNode){
        return this.handler.loadNode(blockNode);
    }

    public BlockStream getStream(){
        return this.handler.getStream();
    }

    public RecordInfoExtractor getRecordInterface(){
        return this.handler.getRi();
    }


    public abstract void save();
    public abstract void load();

    public abstract void insert(BigInteger t,ByteBuffer m);
    public abstract ByteBuffer get(BigInteger t);
    public abstract ByteBuffer remove(BigInteger t);

    //Pega metade dos dados do nó atual e retorna um novo nó com dados do antigo nó
    protected abstract Node half();
    public abstract Node merge(Node node);


    public abstract void print(int tabs);


    public abstract boolean hasMinimun();

    public abstract boolean isFull();
    public abstract BigInteger min();
    public abstract BigInteger max();

    public abstract int height();

    public abstract Leaf leafFrom(BigInteger key);

    public abstract Iterator<Map.Entry<BigInteger, ByteBuffer>> iterator(BigInteger pk);


    protected static <T,J> Map.Entry<T,J> makeEntry(T key, J buff){
        return new Map.Entry<T, J>() {
            T k = key;
            J b =buff;

            @Override
            public T getKey() {
                return k;
            }

            @Override
            public J getValue() {
                return b;
            }

            @Override
            public J setValue(J value) {
                b = value;
                return b;
            }
        };
    }
}
