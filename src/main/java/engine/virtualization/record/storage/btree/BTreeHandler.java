package engine.virtualization.record.storage.btree;

import engine.exceptions.DataBaseException;
import engine.file.blocks.ReadableBlock;
import engine.file.streams.BlockStream;
import engine.virtualization.interfaces.BlockManager;
import engine.virtualization.record.RecordInfoExtractor;

import java.util.TreeMap;

public class BTreeHandler {

    private BlockManager blockManager;
    protected BlockStream stream;
    protected RecordInfoExtractor extractor;
    protected TreeMap<Integer,Node> nodeRelation;

    private int sizeOfPk;
    private int sizeOfEntry;

    public BTreeHandler(BlockStream stream,BlockManager blockManager,RecordInfoExtractor extractor, int sizeOfPk, int sizeOfEntry){
        this.blockManager=blockManager;
        this.sizeOfEntry=sizeOfEntry;
        this.extractor = extractor;
        this.sizeOfPk=sizeOfPk;
        this.stream = stream;
        this.nodeRelation= new TreeMap<>();
    }

    public Node loadNode(int blockNode){
        if(nodeRelation.get(blockNode)!=null)
            return nodeRelation.get(blockNode);
        if(blockNode<0)
            throw new DataBaseException("BTree->Node->loadNode","Node negativo");
        ReadableBlock rb = getStream().getBlockReadByteStream(blockNode);
        byte type = rb.read(0,1).get(0);
        Node node;
        switch (type){
            case 1:
                node = new Leaf(this,blockNode);
                break;
            case 2:
                node = new Page(this,blockNode,null);
                break;
            case -1:
                throw new DataBaseException("BTree->Node->loadNode","Tentou ler um bloco base proibido");
            default:
                throw new DataBaseException("BTree->Node->loadNode","Tipo do node não reconhecido");
        }
        node.load();
        nodeRelation.put(blockNode,node);
        return node;
    }

    public BlockManager getBlockManager() {
        return blockManager;
    }

    public int getSizeOfPk() {
        return sizeOfPk;
    }

    public int getSizeOfEntry() {
        return sizeOfEntry;
    }

    public BlockStream getStream() {
        return stream;
    }

    public RecordInfoExtractor getInfoExtractor(){
        return extractor;
    }

}
