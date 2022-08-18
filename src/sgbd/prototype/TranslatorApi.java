package sgbd.prototype;

import engine.exceptions.DataBaseException;
import engine.file.streams.ReadByteStream;
import engine.util.Util;
import engine.virtualization.record.Record;
import engine.virtualization.record.RecordInfoExtractor;
import engine.virtualization.record.instances.GenericRecord;
import sgbd.util.UtilConversor;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class TranslatorApi implements RecordInfoExtractor, Iterable<Column>{

    private final int headerSize;
    private byte[] headerBuffer;


    private final ArrayList<Column> columns;
    private final HashMap<Integer,Integer> headerPosition;

    private final int primaryKeySize;
    private byte[] bufferArrayPk;

    protected TranslatorApi(ArrayList<Column> columns){
        this.columns=columns;
        int headerSize = 1;
        this.headerPosition = new HashMap<>();
        /*
        0 - 256
                0-253 -> ja é o tamanho => 1 byte
                254 -> deve ler um short => 3 byes
                255 -> deve ler um int   => 5 bytes => unico pior caso
                5 bytes
                30 = nome -> 14 => 1 byte
                44 = email -> 20 => 1 byte
                64 = descricao -> 500 => 3 bytes
                //
                12 bytes
                int == 4 bytes
                [int 30,int 44,int 64]
                30 = nome -> 14
                44 = email -> 20
                64 = descricao -> 500
        */
        int aux=1;
        int sizePk = 0;
        for (Column c:columns){
            if(c.camBeNull()){
                this.headerPosition.put(columns.indexOf(c),(headerSize-1)*8+aux);
                aux++;
            }
            if(aux>=8){
                headerSize++;
                aux=0;
            }
            if(c.isPrimaryKey()){
                sizePk +=c.getSize();
            }
        }
        this.headerSize = headerSize;
        primaryKeySize = sizePk;
        bufferArrayPk = new byte[sizePk];
        headerBuffer = new byte[headerSize];
    }

    public int maxRecordSize(){
        int size = headerSize;
        for(Column c: columns){
            size+=c.getSize();
            if(c.isDinamicSize())size+=4;
        }
        return size;
    }

    public BigInteger getPrimaryKey(RowData rw){
        BigInteger pk;
        ByteBuffer buffer = ByteBuffer.allocate(primaryKeySize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for(Column c:columns){
            if(c.isPrimaryKey()){
                byte[] arr = rw.getData(c.getName());
                buffer.put(arr);
            }else{
                break;
            }
        }
        return Util.convertByteArrayToNumber(buffer.array());
    }

    public int getPrimaryKeySize(){
        return primaryKeySize;
    }

    @Override
    public synchronized BigInteger getPrimaryKey(ByteBuffer rbs) {
        rbs.get(this.headerSize,bufferArrayPk,0,primaryKeySize);
        return Util.convertByteArrayToNumber(bufferArrayPk);
    }

    @Override
    public synchronized BigInteger getPrimaryKey(ReadByteStream rbs) {
        rbs.read(this.headerSize,bufferArrayPk,0,primaryKeySize);
        return Util.convertByteArrayToNumber(bufferArrayPk);
    }

    @Override
    public boolean isActiveRecord(ByteBuffer rbs) {
        return false;
    }

    @Override
    public synchronized boolean isActiveRecord(ReadByteStream rbs) {
        rbs.read(0,bufferArrayPk,0,1);
        return (bufferArrayPk[0]&0x1) !=0;
    }

    @Override
    public void setActiveRecord(Record r, boolean active) {
        byte[] arr = r.getData();
        arr[0] = (byte)( (arr[0]&(~0x1)) | ((active)?0x1:0x0));
    }

    public void validateRowData(RowData rw){
        if(rw.isValid())return;
        for(Column c: columns){
            byte[] data = rw.getData(c.getName());
            if(data == null){
                if(!c.camBeNull()){
                    throw new DataBaseException("RecordTranslateApi->convertToRecord","Coluna "+c.getName()+" não pode ser nula!");
                }else if(c.isPrimaryKey()){
                    throw new DataBaseException("RecordTranslateApi->convertToRecord","Coluna "+c.getName()+" não pode ser nula!");
                }
            }else{
                if(c.isDinamicSize()){
                    if(data.length>c.getSize()){
                        throw new DataBaseException("RecordTranslateApi->convertToRecord","Dado passado para a coluna "+c.getName()+" é maior que o limite: "+c.getSize());
                    }
                }else{
                    if(data.length>c.getSize()){
                        throw new DataBaseException("RecordTranslateApi->convertToRecord","Dado passado para a coluna "+c.getName()+" é diferente do tamanho fixo: "+c.getSize());
                    }
                }
            }
        }
        rw.setValid();
    }


    public synchronized ComplexRowData convertToRowData(Record r){
        ComplexRowData row = new ComplexRowData();
        byte[] data = r.getData();
        byte[] header = headerBuffer;
        System.arraycopy(data,0,header,0,this.headerSize);
        int headerPointer = 1;
        int offset = this.headerSize;

        for(Column c: columns){
            if(c.camBeNull()){
                try {
                    if ((header[headerPointer / 8] & (1 << headerPointer%8)) != 0) {
                        //campo é nulo
                        continue;
                    }
                }finally {
                    headerPointer++;
                }
            }
            if(c.isDinamicSize()){
                int size = UtilConversor.byteArrayToInt(Arrays.copyOfRange(data,offset,offset+4));
                offset+=4;
                byte[] arr = Arrays.copyOfRange(data,offset,offset+size);
                offset+=size;
                row.setData(c.getName(),arr,c);
            }else{
                byte[] arr = Arrays.copyOfRange(data,offset,offset+c.getSize());
                offset+=c.getSize();
                row.setData(c.getName(),arr,c);
            }
        }
        return row;
    }


    public synchronized ComplexRowData convertToRowData(Record r, List<String> select){
        ComplexRowData row = new ComplexRowData();
        byte[] data = r.getData();
        byte[] header = headerBuffer;
        System.arraycopy(data,0,header,0,this.headerSize);
        int headerPointer = 1;
        int offset = this.headerSize;
        int selecteds = 0;

        for(Column c: columns){
            if(selecteds >= select.size())break;
            boolean checkColumn = select.contains(c.getName());
            if(checkColumn)selecteds++;
            if(c.camBeNull()){
                try {
                    if ((header[headerPointer / 8] & (1 << headerPointer%8)) != 0) {
                        //campo é nulo
                        continue;
                    }
                }finally {
                    headerPointer++;
                }
            }
            if (c.isDinamicSize()) {
                int size = UtilConversor.byteArrayToInt(Arrays.copyOfRange(data, offset, offset + 4));
                offset += 4;
                if(checkColumn) {
                    byte[] arr = Arrays.copyOfRange(data, offset, offset + size);
                    row.setData(c.getName(), arr,c);
                }
                offset += size;
            } else {
                if(checkColumn) {
                    byte[] arr = Arrays.copyOfRange(data, offset, offset + c.getSize());
                    row.setData(c.getName(), arr,c);
                }
                offset += c.getSize();
            }
        }
        return row;
    }

    public Record convertToRecord(RowData rw){
        this.validateRowData(rw);
        byte[] header = new byte[this.headerSize];
        ArrayList<byte[]> dados = new ArrayList<>();
        int size = this.headerSize;
        header[0] |= 1;
        for(Column c: columns){
            byte[] data = rw.getData(c.getName());
            if(data == null){
                if(c.camBeNull()){
                    int posHeader = headerPosition.get(columns.indexOf(c));
                    header[posHeader/8] |= 1<<(posHeader%8);
                }
            }else{
                if(c.isDinamicSize()){
                    ByteBuffer buff = ByteBuffer.allocate(4);
                    buff.order(ByteOrder.LITTLE_ENDIAN);
                    buff.putInt(data.length);
                    byte[] indice = buff.array();
                    dados.add(indice);
                    dados.add(data);
                    size+=indice.length;
                    size+=data.length;
                }else{
                    dados.add(data);
                    size+=c.getSize();
                    if(data.length<c.getSize()){
                        dados.add(new byte[c.getSize()-data.length]);
                    }
                }
            }
        }
        byte[] bufferRecord = new byte[size];
        int offset = this.headerSize;
        System.arraycopy(header,0,bufferRecord,0,header.length);
        for(byte[] data:dados){
            System.arraycopy(data,0,bufferRecord,offset,data.length);
            offset+=data.length;
        }
        return new GenericRecord(bufferRecord);
    }

    @Override
    public Iterator<Column> iterator() {
        return columns.iterator();
    }
}
