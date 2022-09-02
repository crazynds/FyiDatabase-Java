package engine.virtualization.record.manager.storage.btree;

import engine.file.blocks.ReadableBlock;
import engine.file.streams.ReferenceReadByteStream;
import engine.file.streams.WriteByteStream;
import engine.util.Util;
import lib.btree.BPlusTreeInsertionException;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class Leaf extends Node{

    private int sizeOfEntry,sizeOfPk;

    private int itens,maxItens;
    private int nextLeaf;
    private TreeMap<BigInteger, Map.Entry<Integer,ByteBuffer>> mapPosition;

    private boolean changed;

    public Leaf(BTreeHandler handler, int block) {
        super(handler,block);
        this.sizeOfPk = handler.getSizeOfPk();
        this.sizeOfEntry = handler.getSizeOfEntry();
        this.itens = 0;
        this.nextLeaf = -1;
        this.mapPosition = new TreeMap<>();
        this.changed = true;

        // (tamanho do bloco - 9 bytes de headers) /sizeOfEntry
        maxItens = (getStream().getBlockSize() - 9)/sizeOfEntry;
    }

    @Override
    public void save() {
        if(!changed)return;
        ReadableBlock readable = getStream().getBlockReadByteStream(block);
        WriteByteStream wbs = getStream().getBlockWriteByteStream(block);
        wbs.setPointer(0);
        wbs.writeSeq(new byte[]{1},0,1);
        wbs.writeSeq(Util.convertLongToByteArray(itens,4),0,4);
        wbs.writeSeq(Util.convertLongToByteArray(nextLeaf,4),0,4);

        int position = (int)wbs.getPointer();
        ByteBuffer bufferAux = ByteBuffer.allocate(sizeOfEntry);
        for(Map.Entry<BigInteger,Map.Entry<Integer,ByteBuffer>> entry:mapPosition.entrySet()){
            Map.Entry<Integer,ByteBuffer> value = entry.getValue();
            if(value.getValue()!=null || value.getKey()!=position){
                ByteBuffer buff;
                if(value.getValue()==null){
                    readable.read(value.getKey(),bufferAux,0,sizeOfEntry);
                    buff = bufferAux;
                }else{
                    buff = value.getValue();
                }
                entry.setValue(makeEntry(position,null));
                wbs.write(position,buff.array(),buff.capacity());
            }
            position+=sizeOfEntry;
        }
        wbs.commitWrites();

        changed = false;
    }

    @Override
    public void load() {
        ReadableBlock readable = getStream().getBlockReadByteStream(block);

        readable.setPointer(1);
        itens = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
        nextLeaf = Util.convertByteBufferToNumber(readable.readSeq(4)).intValue();
        ReferenceReadByteStream ref = new ReferenceReadByteStream(readable,readable.getPointer());
        for(int x=0;x<itens;x++){
            mapPosition.put(
                    getRecordInterface().getPrimaryKey(ref),
                    makeEntry((int)ref.getOffset(),(ByteBuffer) null)
            );
            ref.setOffset(ref.getOffset()+sizeOfEntry);
        }
        changed=false;
    }

    @Override
    public void insert(BigInteger t, ByteBuffer m) {
        if(mapPosition.containsKey(t)){
            // Se contem um com essa key faz o replace
            Map.Entry<Integer,ByteBuffer> entry=  mapPosition.get(t);
            if(entry.getValue()==null){
                ByteBuffer buff = ByteBuffer.allocate(sizeOfEntry);
                buff.put(0,m,0,m.capacity());
                entry.setValue(buff);
            }else{
                entry.getValue().put(0,m,0,m.capacity());
            }
        }else {
            if(mapPosition.size()>=maxItens){
                // Se já ta no limite chama para o pai desse cara
                throw new BPlusTreeInsertionException(Map.entry(t,m));
            }

            // Se tem espaço e não tem nenhuma igual adiciona o item na memoria.
            mapPosition.put(t,makeEntry(0,m.duplicate()));
            itens++;
        }
        changed=true;
    }

    private Map.Entry<Integer,ByteBuffer> makeEntry(Integer key,ByteBuffer buff){
        return new Map.Entry<Integer, ByteBuffer>() {
            Integer k = key;
            ByteBuffer b =buff;

            @Override
            public Integer getKey() {
                return k;
            }

            @Override
            public ByteBuffer getValue() {
                return b;
            }

            @Override
            public ByteBuffer setValue(ByteBuffer value) {
                b = value;
                return b;
            }
        };
    }

    @Override
    public ByteBuffer get(BigInteger t) {
        Map.Entry<Integer,ByteBuffer> entry = mapPosition.get(t);
        if(entry!=null)return null;

        if(entry.getKey()==0)return entry.getValue();
        ReadableBlock readable = getStream().getBlockReadByteStream(block);
        return readable.read(entry.getKey(),sizeOfEntry);
    }

    @Override
    public ByteBuffer remove(BigInteger t) {
        Map.Entry<Integer,ByteBuffer> entry = mapPosition.remove(t);
        if(entry!=null)return null;
        itens--;
        changed=true;

        if(entry.getKey()==0)return entry.getValue();
        ReadableBlock readable = getStream().getBlockReadByteStream(block);
        return readable.read(entry.getKey(),sizeOfEntry);
    }

    @Override
    protected Node half() {
        Leaf left = this;
        Leaf right = new Leaf(this.handler,this.handler.getBlockManager().allocNew());
        ReadableBlock readable = getStream().getBlockReadByteStream(block);

        int savedQtd = itens;

        for(int x=itens/2;x<savedQtd;x++){
            Map.Entry<BigInteger, Map.Entry<Integer,ByteBuffer>> aux = mapPosition.pollLastEntry();
            if(aux.getValue().getValue()!=null){
                right.insert(aux.getKey(),aux.getValue().getValue());
            }else{
                right.insert(aux.getKey(),readable.read(aux.getValue().getKey(),sizeOfEntry));
            }
            itens--;
        }
        right.setNextLeaf(left.getNextLeaf());
        left.setNextLeaf(right.block);
        return right;
    }

    private void setNextLeaf(int block){
        this.nextLeaf = block;
    }

    public int getNextLeaf(){
        return this.nextLeaf;
    }

    @Override
    public Node merge(Node node) {
        return null;
    }

    @Override
    public void print(int tabs) {
        for(Map.Entry<BigInteger, Map.Entry<Integer,ByteBuffer>> e:mapPosition.entrySet()){
            for(int x=0;x<tabs;x++)System.out.print("\t");
            System.out.println("PK: "+e.getKey()+" | Bloco: "+block+" | Pos: "+e.getValue().getKey());
        }
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
