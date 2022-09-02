package engine.virtualization.record.manager.storage.btree;

import engine.exceptions.DataBaseException;
import engine.file.blocks.ReadableBlock;
import engine.file.buffers.BlockBuffer;
import engine.file.streams.BlockStream;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.RecordInterface;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public abstract class Node {

    protected int block;
    protected BTreeHandler handler;

    public Node(BTreeHandler handler, int block){
        this.block = block;
        this.handler=handler;
    }

    public Node loadNode(int blockNode){
        ReadableBlock rb = getStream().getBlockReadByteStream(blockNode);
        byte type = rb.read(0,1).get(0);
        Node node;
        switch (type){
            case 1:
                node = new Leaf(handler,blockNode);
                break;
            case 2:
                node = new Page(handler,blockNode,null);
                break;
            case -1:
                throw new DataBaseException("BTree->Node->loadNode","Tentou ler um bloco base proibido");
            default:
                throw new DataBaseException("BTree->Node->loadNode","Tipo do node não reconhecido");
        }
        node.load();
        return node;
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

    // Funções responsaveis por IO no arquivo


}
