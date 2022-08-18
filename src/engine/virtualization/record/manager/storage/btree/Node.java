package engine.virtualization.record.manager.storage.btree;

import engine.exceptions.DataBaseException;
import engine.file.blocks.Block;
import engine.file.blocks.BlockID;
import engine.file.blocks.ReadableBlock;
import engine.file.buffers.BlockBuffer;
import engine.virtualization.interfaces.BlockManager;
import engine.virtualization.record.RecordInterface;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public abstract class Node {

    protected int block;
    protected BlockBuffer stream;
    protected RecordInterface ri;
    protected BTreeHandler handler;

    public Node(BlockBuffer stream, RecordInterface ri,BTreeHandler handler, int block){
        this.stream=stream;
        this.ri=ri;
        this.block = block;
        this.handler=handler;
    }

    public Node loadNode(int blockNode){
        ReadableBlock rb = stream.getBlockReadByteStream(blockNode);
        byte type = rb.read(0,1).get(0);
        Node node;
        switch (type){
            case 1:
                node = new Leaf(stream,ri,handler,blockNode);
                break;
            case 2:
                node = new Page(stream,ri,handler,blockNode);
                break;
            default:
                throw new DataBaseException("BTree->Node->loadNode","Tipo do node não reconhecido");
        }
        return node;
    }

    public abstract void save();
    public abstract void load();

    public abstract void insert(BigInteger t,ByteBuffer m);
    public abstract ByteBuffer get(BigInteger t);
    public abstract ByteBuffer remove(BigInteger t);

    //Pega metade dos dados do nó atual e retorna um novo nó com dados do antigo nó
    protected abstract Node half();
    public abstract Node merge(Node node);


    public abstract boolean hasMinimun();

    public abstract boolean isFull();
    public abstract BigInteger min();
    public abstract BigInteger max();

    public abstract int height();

    public abstract Leaf leafFrom(BigInteger key);

    // Funções responsaveis por IO no arquivo


}
