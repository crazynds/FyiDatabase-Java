package engine.virtualization.record.manager.storage.btree;

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

public class Leaf extends Node{

    private int sizeOfEntry,sizeOfPk;

    private int itens,maxItens;
    private int nextLeaf;
    private TreeMap<BigInteger, Map.Entry<Long,ByteBuffer>> mapPosition;

    private boolean changed = false;

    public Leaf(BlockBuffer stream, RecordInterface ri,BTreeHandler handler, int block) {
        super(stream,ri,handler,block);
        this.sizeOfPk = handler.getSizeOfPk();
        this.sizeOfEntry = handler.getSizeOfEntry();
        this.itens = 0;
        this.nextLeaf = -1;
        this.mapPosition = new TreeMap<>();

        // (tamanho do bloco - 9 bytes de headers) /sizeOfEntry
        maxItens = (stream.getBlockSize() - 9)/sizeOfEntry;
    }

    @Override
    public void save() {
        if(!changed)return;
        ReadableBlock readable = stream.getBlockReadByteStream(block);
        WriteByteStream wbs = stream.getBlockWriteByteStream(block);
        wbs.setPointer(0);
        wbs.writeSeq(new byte[]{1},0,1);
        wbs.writeSeq(Util.convertLongToByteArray(itens,4),0,4);
        wbs.writeSeq(Util.convertLongToByteArray(nextLeaf,4),0,4);

        int position = (int)wbs.getPointer();
        ByteBuffer bufferAux = ByteBuffer.allocate(sizeOfEntry);
        for(Map.Entry<BigInteger,Map.Entry<Long,ByteBuffer>> entry:mapPosition.entrySet()){
            Map.Entry<Long,ByteBuffer> value = entry.getValue();
            if(value.getValue()!=null || value.getKey()!=position){
                ByteBuffer buff = null;
                if(value.getValue()==null){
                    readable.read(value.getKey(),bufferAux,0,sizeOfEntry);
                    buff = bufferAux;
                }else{
                    buff = value.getValue();
                }
                wbs.write(position,buff.array(),buff.capacity());
            }
            position+=sizeOfEntry;
        }
        wbs.commitWrites();

        changed = false;
    }

    @Override
    public void load() {
        ReadableBlock readable = stream.getBlockReadByteStream(block);

        readable.setPointer(1);
        itens = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
        nextLeaf = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
        ReferenceReadByteStream ref = new ReferenceReadByteStream(readable,readable.getPointer());
        for(int x=0;x<itens;x++){
            mapPosition.put(
                    ri.getExtractor().getPrimaryKey(ref),
                    Map.entry(ref.getReference(),(ByteBuffer) null)
            );
            ref.setOffset(ref.getReference()+sizeOfEntry);
        }
        changed=false;
    }

    @Override
    public void insert(BigInteger t, ByteBuffer m) {
        if(mapPosition.containsKey(t)){
            // Se contem um com essa key faz o replace
            Map.Entry<Long,ByteBuffer> entry=  mapPosition.get(t);
            if(entry.getValue()==null){
                ByteBuffer buff = ByteBuffer.allocate(sizeOfEntry);
                buff.put(0,m,0,m.capacity());
                entry.setValue(buff);
            }else{
                entry.getValue().put(0,m,0,m.capacity());
            }
        }else {
            if(mapPosition.size()>=maxItens){
                // Se j� ta no limite chama para o pai desse cara
                throw new BPlusTreeInsertionException(Map.entry(t,m));
            }

            // Se tem espa�o e n�o tem nenhuma igual adiciona o item na memoria.
            mapPosition.put(t,Map.entry((long)0,m.duplicate()));
            itens++;
        }
        changed=true;
    }

    @Override
    public ByteBuffer get(BigInteger t) {
        Map.Entry<Long,ByteBuffer> entry = mapPosition.get(t);
        if(entry!=null)return null;

        if(entry.getKey()==0)return entry.getValue();
        ReadableBlock readable = stream.getBlockReadByteStream(block);
        return readable.read(entry.getKey(),sizeOfEntry);
    }

    @Override
    public ByteBuffer remove(BigInteger t) {
        Map.Entry<Long,ByteBuffer> entry = mapPosition.remove(t);
        if(entry!=null)return null;
        itens--;
        changed=true;

        if(entry.getKey()==0)return entry.getValue();
        ReadableBlock readable = stream.getBlockReadByteStream(block);
        return readable.read(entry.getKey(),sizeOfEntry);
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
        return itens>= maxItens/2;
    }

    @Override
    public boolean isFull() {
        return itens>=maxItens;
    }

    @Override
    public BigInteger min() {
        return mapPosition.firstKey();
    }

    @Override
    public BigInteger max() {
        return mapPosition.lastKey();
    }

    @Override
    public int height() {
        return 0;
    }

    @Override
    public Leaf leafFrom(BigInteger key) {
        return this;
    }

}
